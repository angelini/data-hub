/*

https://app.sqldbm.com/PostgreSQL/Edit/p101568/
https://api.elephantsql.com/console/2ff47245-6a07-4ada-a1a2-6adf3f700b23/details

*/

DROP TABLE IF EXISTS hubs               CASCADE;
DROP TABLE IF EXISTS datasets           CASCADE;
DROP TABLE IF EXISTS columns            CASCADE;
DROP TABLE IF EXISTS types              CASCADE;
DROP TABLE IF EXISTS backends           CASCADE;
DROP TABLE IF EXISTS dataset_versions   CASCADE;
DROP TABLE IF EXISTS dependencies       CASCADE;
DROP TABLE IF EXISTS published_versions CASCADE;
DROP TABLE IF EXISTS partitions         CASCADE;

DROP TABLE IF EXISTS users        CASCADE;
DROP TABLE IF EXISTS roles        CASCADE;
DROP TABLE IF EXISTS user_roles   CASCADE;
DROP TABLE IF EXISTS admin_access CASCADE;
DROP TABLE IF EXISTS read_access  CASCADE;
DROP TABLE IF EXISTS write_access CASCADE;

DROP INDEX IF EXISTS partitions_values_idx;


CREATE TABLE IF NOT EXISTS hubs (
    id         uuid,

    name       text      UNIQUE,
    hive_host  text,
    created_at timestamp WITH time zone,

    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datasets (
    hub_id     uuid,
    id         uuid,

    name       text      NOT NULL,
    created_at timestamp WITH time zone NOT NULL,
    deleted_at timestamp WITH time zone,

    PRIMARY KEY (hub_id, id),
    FOREIGN KEY (hub_id) REFERENCES hubs(id)
);

CREATE TABLE IF NOT EXISTS backends (
    id int,

    module text NOT NULL UNIQUE,

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dataset_versions (
    hub_id      uuid,
    dataset_id  uuid,
    version     int,

    backend_id     int       NOT NULL,
    partition_keys text[]    NOT NULL,
    path           text      NOT NULL,
    description    text      NOT NULL,
    is_overlapping boolean   NOT NULL,
    created_at     timestamp WITH time zone NOT NULL,

    PRIMARY KEY (hub_id, dataset_id, version),
    FOREIGN KEY (hub_id, dataset_id) REFERENCES datasets(hub_id, id),
    FOREIGN KEY (backend_id) REFERENCES backends(id)
);

CREATE TABLE IF NOT EXISTS dependencies (
    parent_hub_id     uuid,
    parent_dataset_id uuid,
    parent_version    int,
    child_hub_id      uuid,
    child_dataset_id  uuid,
    child_version     int
);

CREATE TABLE IF NOT EXISTS types (
    id        int,

    name      text NOT NULL,
    hive_type text NOT NULL,

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS columns (
    hub_id     uuid,
    dataset_id uuid,
    version    int,
    name       text,

    type_id     int     NOT NULL,
    position    int     NOT NULL,
    description text    NOT NULL,
    is_nullable boolean NOT NULL,
    is_unique   boolean NOT NULL,
    has_pii     boolean NOT NULL,

    PRIMARY KEY (hub_id, dataset_id, version, name),
    FOREIGN KEY (hub_id, dataset_id, version) REFERENCES dataset_versions(hub_id, dataset_id, version),
    FOREIGN KEY (type_id) REFERENCES types(id)
);

CREATE TABLE IF NOT EXISTS published_versions (
    hub_id       uuid,
    dataset_id   uuid,
    version      int,

    published_at time WITH time zone,

    PRIMARY KEY (hub_id, dataset_id, version),
    FOREIGN KEY (hub_id, dataset_id, version) REFERENCES dataset_versions(hub_id, dataset_id, version)
);

CREATE TABLE IF NOT EXISTS partitions (
    hub_id            uuid,
    dataset_id        uuid,
    version           int,
    id                uuid,

    partition_values text[],
    path             text      NOT NULL,
    row_count        int,
    start_time       timestamp WITH time zone,
    end_time         timestamp WITH time zone,
    created_at       timestamp WITH time zone NOT NULL,
    deleted_at       timestamp WITH time zone,

    PRIMARY KEY (hub_id, dataset_id, version, id),
    FOREIGN KEY (hub_id, dataset_id, version) REFERENCES dataset_versions(hub_id, dataset_id, version)
);

CREATE INDEX partitions_values_idx ON partitions USING gin(partition_values);

CREATE TABLE IF NOT EXISTS users (
    id    uuid,

    email text NOT NULL,

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS roles (
    id    uuid,

    name text NOT NULL,

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id    uuid,
    role_id    uuid,

    created_at timestamp WITH time zone NOT NULL,
    deleted_at timestamp WITH time zone,

    PRIMARY KEY (user_id, role_id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (role_id) REFERENCES roles(id)
);

CREATE TABLE IF NOT EXISTS admin_access (
    role_id    uuid,
    hub_id     uuid,
    id         uuid,

    created_at timestamp WITH time zone NOT NULL,
    revoked_at timestamp WITH time zone,

    PRIMARY KEY (role_id, hub_id, id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (hub_id) REFERENCES hubs(id)
);

CREATE TABLE IF NOT EXISTS read_access (
    role_id    uuid,
    hub_id     uuid,
    dataset_id uuid,
    id         uuid,

    created_at timestamp WITH time zone NOT NULL,
    revoked_at timestamp WITH time zone,

    PRIMARY KEY (role_id, hub_id, id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (hub_id, dataset_id) REFERENCES datasets(hub_id, id)
);

CREATE TABLE IF NOT EXISTS write_access (
    role_id    uuid,
    hub_id     uuid,
    dataset_id uuid,
    id         uuid,

    created_at timestamp WITH time zone NOT NULL,
    revoked_at timestamp WITH time zone,

    PRIMARY KEY (role_id, hub_id, id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (hub_id, dataset_id) REFERENCES datasets(hub_id, id)
);

CREATE OR REPLACE VIEW current_published_versions AS
    WITH ranked AS (
        SELECT
            pub.hub_id,
            pub.dataset_id,
            pub.version,
            pub.published_at,
            ROW_NUMBER()
            OVER (
                PARTITION BY pub.hub_id, pub.dataset_id
                ORDER BY pub.published_at DESC
            ) idx
        FROM published_versions pub
    )
    SELECT
        ran.hub_id,
        ran.dataset_id,
        ran.version,
        ran.published_at
    FROM ranked ran
    WHERE ran.idx = 1;

CREATE OR REPLACE VIEW datasets_with_current_versions AS
    SELECT
        dat.hub_id,
        dat.id,
        dat.name,
        cur.version,
        dat.created_at,
        cur.published_at
    FROM
        datasets dat
    LEFT JOIN
        current_published_versions cur ON dat.id = cur.dataset_id
    WHERE dat.deleted_at IS NULL;

CREATE OR REPLACE VIEW versions_with_backend AS
    SELECT
        ver.hub_id,
        ver.dataset_id,
        ver.version,
        ver.partition_keys,
        bac.module,
        ver.path,
        ver.description,
        ver.created_at
    FROM
        dataset_versions ver
    INNER JOIN
        backends bac ON ver.backend_id = bac.id;

CREATE OR REPLACE VIEW columns_with_type AS
    SELECT
        col.hub_id,
        col.dataset_id,
        col.version,
        col.name,
        typ.name as type_name,
        col.position,
        col.description,
        col.is_nullable,
        col.is_unique,
        col.has_pii
    FROM
        columns col
    INNER JOIN
        types typ ON col.type_id = typ.id;
