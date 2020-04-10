/*

https://app.sqldbm.com/PostgreSQL/Edit/p101568/

*/

DROP TABLE IF EXISTS users              CASCADE;
DROP TABLE IF EXISTS teams              CASCADE;
DROP TABLE IF EXISTS team_members       CASCADE;
DROP TABLE IF EXISTS hubs               CASCADE;
DROP TABLE IF EXISTS team_roles         CASCADE;
DROP TABLE IF EXISTS datasets           CASCADE;
DROP TABLE IF EXISTS backends           CASCADE;
DROP TABLE IF EXISTS dataset_versions   CASCADE;
DROP TABLE IF EXISTS columns            CASCADE;
DROP TABLE IF EXISTS types              CASCADE;
DROP TABLE IF EXISTS dependencies       CASCADE;
DROP TABLE IF EXISTS published_versions CASCADE;
DROP TABLE IF EXISTS partitions         CASCADE;
DROP TABLE IF EXISTS connectors         CASCADE;
DROP TABLE IF EXISTS connections        CASCADE;

DROP TYPE IF EXISTS access_level CASCADE;

CREATE TABLE IF NOT EXISTS users (
    id uuid,

    email         text        NOT NULL,
    password_hash text        NOT NULL,
    created_at    timestamptz NOT NULL,

    PRIMARY KEY (id),
    CONSTRAINT email_length CHECK (char_length(email) >= 2 AND char_length(email) < 1028)
);

CREATE TABLE IF NOT EXISTS teams (
    id uuid,

    name       text        NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,

    PRIMARY KEY (id),
    CONSTRAINT name_length CHECK (char_length(name) >= 2 AND char_length(name) < 1028)
);

CREATE TABLE IF NOT EXISTS team_members (
    id uuid,

    team_id    uuid        NOT NULL,
    user_id    uuid        NOT NULL,
    created_at timestamptz NOT NULL,
    deleted_at timestamptz,

    PRIMARY KEY (id),
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT deleted_at_greater CHECK (deleted_at IS NULL OR deleted_at > created_at)
);

CREATE INDEX team_members_team_user_idx ON team_members(team_id, user_id);

CREATE TABLE IF NOT EXISTS hubs (
    id uuid,

    team_id    uuid        NOT NULL,
    name       text        NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,

    PRIMARY KEY(id),
    FOREIGN KEY (team_id) REFERENCES teams(id),
    CONSTRAINT name_length CHECK (char_length(name) >= 2 AND char_length(name) < 1028)
);

CREATE TYPE access_level AS ENUM ('none', 'reader', 'writer', 'admin');

CREATE TABLE IF NOT EXISTS team_roles (
    id uuid,

    team_id      uuid         NOT NULL,
    hub_id       uuid         NOT NULL,
    access_level access_level NOT NULL,
    created_at   timestamptz  NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (hub_id) REFERENCES hubs(id)
);

CREATE INDEX team_roles_team_hub_idx ON team_roles(team_id, hub_id);

CREATE TABLE IF NOT EXISTS datasets (
    hub_id uuid,
    id     uuid,

    name       text        NOT NULL,
    created_at timestamptz NOT NULL,
    deleted_at timestamptz,

    PRIMARY KEY (hub_id, id),
    FOREIGN KEY (hub_id) REFERENCES hubs(id),
    CONSTRAINT name_length CHECK (char_length(name) >= 2 AND char_length(name) < 1028),
    CONSTRAINT deleted_at_greater CHECK (deleted_at IS NULL OR deleted_at > created_at)
);

CREATE TABLE IF NOT EXISTS backends (
    id int,

    module text NOT NULL UNIQUE,

    PRIMARY KEY (id),
    CONSTRAINT valid_python_module CHECK (module ~ '^[A-Za-z_][A-Za-z0-9_.]*')
);

CREATE TABLE IF NOT EXISTS dataset_versions (
    hub_id      uuid,
    dataset_id  uuid,
    version     int,

    backend_id     int         NOT NULL,
    partition_keys text[]      NOT NULL,
    path           text        NOT NULL,
    description    text        NOT NULL,
    is_overlapping boolean     NOT NULL,
    created_at     timestamptz NOT NULL,

    PRIMARY KEY (hub_id, dataset_id, version),
    FOREIGN KEY (hub_id, dataset_id) REFERENCES datasets(hub_id, id),
    FOREIGN KEY (backend_id) REFERENCES backends(id),
    CONSTRAINT positive_version CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS dependencies (
    parent_hub_id     uuid,
    parent_dataset_id uuid,
    parent_version    int,
    child_hub_id      uuid,
    child_dataset_id  uuid,
    child_version     int,

    PRIMARY KEY (parent_hub_id, parent_dataset_id, parent_version, child_hub_id, child_dataset_id, child_version),
    FOREIGN KEY (parent_hub_id, parent_dataset_id, parent_version) REFERENCES dataset_versions(hub_id, dataset_id, version),
    FOREIGN KEY (child_hub_id, child_dataset_id, child_version) REFERENCES dataset_versions(hub_id, dataset_id, version),
    CONSTRAINT different_versions CHECK (parent_hub_id != child_hub_id OR parent_dataset_id != child_hub_id OR parent_version != child_version)
);

CREATE INDEX child_dependencies_idx ON dependencies(child_hub_id, child_dataset_id, child_version);

CREATE TABLE IF NOT EXISTS types (
    id int,

    name text NOT NULL,

    PRIMARY KEY (id),
    CONSTRAINT name_length CHECK (char_length(name) >= 2 AND char_length(name) < 1028)
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
    FOREIGN KEY (type_id) REFERENCES types(id),
    CONSTRAINT name_length CHECK (char_length(name) >= 1 AND char_length(name) < 1028),
    CONSTRAINT positive_position CHECK (position >= 0)
);

CREATE TABLE IF NOT EXISTS published_versions (
    hub_id     uuid,
    dataset_id uuid,
    version    int,

    published_at timestamptz NOT NULL,

    PRIMARY KEY (hub_id, dataset_id, version),
    FOREIGN KEY (hub_id, dataset_id, version) REFERENCES dataset_versions(hub_id, dataset_id, version)
);

CREATE TABLE IF NOT EXISTS partitions (
    id uuid,

    hub_id           uuid,
    dataset_id       uuid,
    version          int,
    partition_values text[],
    path             text        NOT NULL,
    row_count        int,
    start_time       timestamptz,
    end_time         timestamptz,
    created_at       timestamptz NOT NULL,
    deleted_at       timestamptz,

    PRIMARY KEY (id),
    FOREIGN KEY (hub_id, dataset_id, version) REFERENCES dataset_versions(hub_id, dataset_id, version),
    CONSTRAINT positive_count CHECK (row_count >= 0),
    CONSTRAINT end_time_greater CHECK (start_time IS NULL OR end_time IS NULL OR end_time > start_time),
    CONSTRAINT deleted_at_greater CHECK (deleted_at IS NULL OR deleted_at > created_at)
);

CREATE INDEX partitions_values_idx ON partitions USING gin(partition_values);
CREATE INDEX version_partitions_idx ON partitions(hub_id, dataset_id, version);

CREATE TABLE IF NOT EXISTS connectors (
    id int,

    name     text  NOT NULL,
    config   jsonb,
    template text  NOT NULL,

    PRIMARY KEY (id),
    CONSTRAINT name_length CHECK (char_length(name) >= 1 AND char_length(name) < 1028)
);

CREATE TABLE IF NOT EXISTS connections (
    id uuid,

    hub_id       uuid        NOT NULL,
    dataset_id   uuid        NOT NULL,
    connector_id int         NOT NULL,
    path         text        NOT NULL,
    created_at   timestamptz NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (hub_id, dataset_id) REFERENCES datasets(hub_id, id),
    FOREIGN KEY (connector_id) REFERENCES connectors(id)
);

CREATE OR REPLACE VIEW current_team_members_with_email AS
    WITH ranked AS (
        SELECT
            team_id,
            user_id,
            created_at,
            ROW_NUMBER()
            OVER (
                PARTITION BY team_id, user_id
                ORDER BY created_at DESC
            ) idx
        FROM team_members
        WHERE deleted_at IS NULL
    )
    SELECT
        ran.team_id,
        ran.user_id,
        usr.email AS user_email,
        ran.created_at
    FROM
        ranked ran
    INNER JOIN
        users AS usr
    ON
        ran.user_id = usr.id
    WHERE
        ran.idx = 1;

CREATE OR REPLACE VIEW hubs_with_team_name AS
    SELECT
        hub.id,
        hub.name,
        hub.team_id,
        tea.name AS team_name,
        hub.created_at
    FROM
        hubs hub
    INNER JOIN
        teams tea
    ON
        hub.team_id = tea.id;

CREATE OR REPLACE VIEW current_team_roles_with_name AS
    WITH ranked AS (
        SELECT
            team_id,
            hub_id,
            access_level,
            created_at,
            ROW_NUMBER()
            OVER (
                PARTITION BY team_id, hub_id
                ORDER BY created_at DESC
            ) idx
        FROM team_roles
    )
    SELECT
        ran.team_id,
        ran.hub_id,
        hub.name AS hub_name,
        ran.access_level,
        ran.created_at
    FROM
        ranked ran
    INNER JOIN
        hubs hub
    ON
        ran.hub_id = hub.id
    WHERE
        ran.idx = 1;

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

CREATE OR REPLACE VIEW current_published_versions_with_names AS
    SELECT
        pub.hub_id,
        hub.name AS hub_name,
        pub.dataset_id,
        dat.name AS dataset_name,
        pub.version,
        pub.published_at
    FROM
        current_published_versions as pub
    INNER JOIN hubs hub ON pub.hub_id = hub.id
    INNER JOIN datasets dat ON pub.dataset_id = dat.id;


CREATE OR REPLACE VIEW datasets_with_current_versions AS
    SELECT
        dat.hub_id,
        dat.id,
        dat.name,
        pub.version,
        dat.created_at,
        pub.published_at
    FROM
        datasets dat
    LEFT JOIN
        current_published_versions pub ON dat.id = pub.dataset_id
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

CREATE OR REPLACE VIEW partitions_with_last_end_time AS
    SELECT
        par.hub_id,
        par.dataset_id,
        par.version,
        par.id,
        par.start_time,
        par.end_time,
        LAG(par.end_time, 1) OVER (
            PARTITION BY par.hub_id, par.dataset_id, par.version
            ORDER BY par.start_time
        ) last_end_time
    FROM
        partitions par;


CREATE OR REPLACE VIEW connections_with_connector AS
    SELECT
        cns.id,
        cns.hub_id,
        cns.dataset_id,
        cns.connector_id,
        ctr.name AS connector_name,
        ctr.config AS connector_config,
        ctr.template AS connector_template,
        cns.path,
        cns.created_at
    FROM
        connections as cns
    INNER JOIN
        connectors as ctr
    ON
        cns.connector_id = ctr.id;
