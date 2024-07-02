import duckdb
import os
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import KernelDensity

db = duckdb.connect(os.environ.get("SHEEPDB", "sheep.db"))
db.execute("LOAD spatial;")

def init_v1():
    db.execute('''
        CREATE SCHEMA V1;
        CREATE TABLE V1.Raw (
            source_id BIGINT,
            individual_id BIGINT,
            owner_id BIGINT,
            name VARCHAR,
            position_time TIMESTAMP,
            st_x DOUBLE,
            st_y DOUBLE
        );
        CREATE VIEW V1.Position AS
            SELECT
                source_id AS device_id,
                position_time AS t,
                st_point(st_y, st_x) AS pos
                FROM V1.Raw;
    ''')

def init_v2():
    db.execute('''
        CREATE SCHEMA V2;
        CREATE TABLE V2.Raw (
            MåleID BIGINT,
            Måletidspunkt TIMESTAMP,
            Sekvensnr BIGINT,
            Individnr VARCHAR,
            Telespornr BIGINT,
            Avsender BIGINT,
            Mottaker BIGINT,
            "Posisjon, gyldig" BIGINT,
            Lengdegrad DOUBLE,
            Breddegrad DOUBLE,
            "Posisjon, tid" TIMESTAMP,
            "Debug 1" BIGINT,
            "Debug 2" BIGINT,
            Batterispenning DOUBLE,
            Temperatur BIGINT,
            "Alarm status" BIGINT,
            "Alarm tid" BIGINT,
            "UHF TX Effekt" BIGINT,
            Dispersjonstid BIGINT,
            "Feltstyrke UHF" BIGINT,
            "Feltstyrke VHF" BIGINT,
            "Feltstyrke GSM" BIGINT,
            Kurs VARCHAR,
            Debug BIGINT,
            Firmware BIGINT
        );
        CREATE VIEW V2.Position AS
            SELECT
                Telespornr AS device_id,
                "Posisjon, tid" AS time,
                CASE WHEN "Posisjon, gyldig" = 1
                    THEN st_point(Breddegrad, Lengdegrad)
                    ELSE st_point(NULL, NULL)
                    END AS pos
                FROM V2.Raw;
    ''')

def init_v3():
    db.execute('''
        CREATE SCHEMA V3;
        CREATE TABLE V3.Raw (
            unitno INTEGER,
            platform VARCHAR,
            owner VARCHAR,
            user VARCHAR,
            individualno VARCHAR,
            server_arrival TIMESTAMP_MS,
            latitude VARCHAR,
            langititude VARCHAR,
            position_time TIMESTAMP_MS
        );
        CREATE VIEW V3.Position AS
            SELECT
                unitno AS device_id,
                position_time AS t,
                st_point(
                    TRY_CAST(latitude AS DOUBLE),
                    TRY_CAST(langititude AS DOUBLE)
                ) AS pos
                FROM V3.Raw;
    ''')

def init_sauekontrollen_animals():
    db.execute('''
        CREATE TABLE Animal (
            MEDLEMID BIGINT,
            INDIVID_ID BIGINT,
            UTDATO TIMESTAMP,
            UTKODE_ID INTEGER,
            UTRANGERINGSAARSAK_ID INTEGER
        );
    ''')

def init_db():
   db.execute('''
        CREATE TABLE Owner (
            id BIGINT PRIMARY KEY,
            name VARCHAR
        );
   ''')
   db.execute('''
        CREATE VIEW Position AS
            WITH cte AS (
                FROM V1.Position UNION ALL BY NAME
                FROM V2.Position UNION ALL BY NAME
                FROM V3.Position
            ) SELECT
                device_id::BIGINT AS device_id,
                t::TIMESTAMP AS t,
                pos::GEOMETRY AS pos
                FROM cte;
    ''')

def import_v1(filename):
    db.execute('''
        INSERT INTO V1.Raw BY NAME
            SELECT * FROM read_csv(?, nullstr='(null)', sample_size=-1);
    ''', [filename])

def import_v2(filename):
    db.execute('''
        INSERT INTO V2.Raw BY NAME
            SELECT * FROM read_csv(?, sample_size=-1);
    ''', [filename])

def get_v3_layers(filename):
    rel = db.query("SELECT unnest(layers).name FROM st_read_meta(?)",
        params=[filename]
    )
    return [x for x, in rel.fetchall()]

def import_v3(filename, layer):
    db.execute('''
        INSERT INTO V3.Raw BY NAME
            SELECT * FROM st_read(?, layer=?);
    ''', [filename, layer])

def import_sauekontrollen_animals(filename, layer):
    db.execute('''
        INSERT INTO Animal
            SELECT
            MEDLEMID,
            INDIVID_ID,
            UTDATO,
            UTKODE_ID,
            UTRANGERINGSAARSAK_ID
            FROM st_read(?, layer=?);
    ''', [filename, layer])

def import_all():
    import_v1('data/meraker20*.csv.zst')
    import_v2('data/telespor2.csv.zst')
    import_v3('data/meraker-2022-data.xlsx', layer='jan-july-2022')
    import_v3('data/meraker-2022-data.xlsx', layer='july-dec-2022')
    import_sauekontrollen_animals('data/Sporingsdata Telespor og Sauekontrollen Meråker beitelag 2016-2023 - original.xlsx', 'Dyr_Meråker_Alle 2010-2023')

def get_positions():
    return db.view("Position")

def remove_nullpos(rel):
    return rel.filter("st_x(pos) IS NOT NULL AND st_y(pos) IS NOT NULL")

def remove_duplicates(rel):
    return rel.aggregate("* EXCLUDE (pos), arbitrary(pos) AS pos")

def filter_season(rel):
    return rel.filter("extract('month' FROM t) BETWEEN 6 AND 9")

def identify_trajectories(rel, time_threshold=48*3600, velocity_threshold=10):
    window = "OVER (PARTITION BY device_id ORDER BY t)"
    return (rel
        .project(f'''
            *,
            epoch(age(t, lag(t) {window})) AS dt,
            st_distance_spheroid(pos, lag(pos) {window}) / dt AS velocity,
            CASE WHEN
                dt IS NULL OR
                dt > {time_threshold} OR
                velocity > {velocity_threshold}
                THEN 1 ELSE 0 END
                AS splitflag
        ''')
        .project(f'''
            sum(splitflag) OVER (ORDER BY device_id, t) AS trajectory_id,
            *
        ''')
        .filter("splitflag = 0").project("* EXCLUDE (splitflag)")
    )
