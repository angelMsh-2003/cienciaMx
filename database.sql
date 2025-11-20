-- PROPUESTA DE TABLA

CREATE TABLE repositories (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,                      -- Nombre legible del repositorio
    software        VARCHAR(100),                       -- DSpace, EPrints, etc.
    software_version VARCHAR(50),
    oai_endpoint    TEXT NOT NULL,                      -- endpoint OAI-PMH
    alive           BOOLEAN DEFAULT TRUE,               -- si el endpoint sigue respondiendo
);

CREATE TABLE items (
    id              BIGSERIAL PRIMARY KEY,

    -- Relación con el repositorio
    repository_id   INTEGER NOT NULL REFERENCES repositories(id),

    -- Dublin Core principal
    title           TEXT,                              -- dc:title (unificado a string)
    description     TEXT,                              -- dc:description (puedes unir varios en uno)
    
    -- Fecha
    date_str        TEXT,                              -- valor original (ej. "2016-03-17", "2012", etc.)

    -- Type / Format / Language / Rights (ya unificados)
    dc_type         TEXT,                              -- ej: 'article', 'book', 'thesis' (o el info:eu-repo que decidas)
    dc_format       TEXT,                              -- ej: 'pdf' (tras unificar)
    dc_language     TEXT,                              -- ej: 'Español', 'Inglés', 'spa', etc. según la unificación
    rights_text     TEXT,                              -- texto legible, ej. 'Acceso abierto'
    rights_uri      TEXT,                              -- url de licencia, ej. 'http://creativecommons.org/...'
    subject         TEXT,                              -- lista de subjects unificados (strings)  privisionalmente en una sola columna

    -- Identifier (link principal)
    identifier_url  TEXT,                              -- principal URL (handle u otra)
    identifier_other TEXT[],                           -- otros identifiers (ej. ISBN, segundo handle, etc.)
);


CREATE TABLE creators (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,         -- Nombre nromalizado (ej. "Sanchez Mora, Maria del Carmen")

    -- Datos de autoridad opcionales
    authority_id    TEXT,                  -- valor del @id completo
);

-- Un mismo item puede tener varios creadores y viceversa
CREATE TABLE items_creators (
    item_id         BIGINT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    creator_id      BIGINT NOT NULL REFERENCES creators(id),
    position        SMALLINT NOT NULL DEFAULT 1,   -- orden en la lista de autores

    PRIMARY KEY (item_id, creator_id, position)
);

-- POR VERSE, SI SE INCLUYEN O NO
-- CREATE TABLE subjects (
--     id                BIGSERIAL PRIMARY KEY,
--     value_original    TEXT NOT NULL,    -- texto completo tal cual viene
--     value_normalized  TEXT,             -- opcional, cuando hagas mapeos a algo más legible
--     scheme            TEXT,             -- ej. 'cti', 'LEMB', 'LCSH', 'free', etc.

--     UNIQUE (value_original, scheme)
-- );


-- CREATE TABLE items_subjects (
--     item_id     BIGINT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
--     subject_id  BIGINT NOT NULL REFERENCES subjects(id),

--     PRIMARY KEY (item_id, subject_id)
-- );



