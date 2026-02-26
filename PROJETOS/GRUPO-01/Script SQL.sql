------------------------------------------------------------
-- 1. CRIAÇÃO DAS TABELAS DIMENSÃO
------------------------------------------------------------

CREATE TABLE dim_data (
    id_data SERIAL PRIMARY KEY,
    ano INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    data_completa DATE NOT NULL
);

CREATE TABLE dim_local (
    id_local SERIAL PRIMARY KEY,
    uf CHAR(2) NOT NULL,
    municipio VARCHAR(150) NOT NULL
);

CREATE TABLE dim_causa (
    id_causa SERIAL PRIMARY KEY,
    causa_acidente VARCHAR(255) NOT NULL
);

CREATE TABLE dim_tipo (
    id_tipo SERIAL PRIMARY KEY,
    tipo_acidente VARCHAR(255) NOT NULL
);

CREATE TABLE dim_classificacao (
    id_classificacao SERIAL PRIMARY KEY,
    classificacao_acidente VARCHAR(255) NOT NULL
);

CREATE TABLE dim_fase_dia (
    id_fase SERIAL PRIMARY KEY,
    fase_dia VARCHAR(100) NOT NULL
);

CREATE TABLE dim_clima (
    id_clima SERIAL PRIMARY KEY,
    condicao_metereologica VARCHAR(200) NOT NULL
);

CREATE TABLE dim_pista (
    id_pista SERIAL PRIMARY KEY,
    tipo_pista VARCHAR(150) NOT NULL
);

CREATE TABLE dim_tracado (
    id_tracado SERIAL PRIMARY KEY,
    tracado_via VARCHAR(200) NOT NULL
);

------------------------------------------------------------
-- 2. CRIAÇÃO DA TABELA FATO
------------------------------------------------------------

CREATE TABLE fato_acidente (
    id BIGINT PRIMARY KEY,

    id_data INT,
    id_local INT,
    id_causa INT,
    id_tipo INT,
    id_classificacao INT,
    id_fase INT,
    id_clima INT,
    id_pista INT,
    id_tracado INT,

    ilesos INT,
    feridos_leves INT,
    feridos_graves INT,
    mortos INT,
    total_vitimas INT,
    quantidade_acidentes INT,

    FOREIGN KEY (id_data) REFERENCES dim_data(id_data),
    FOREIGN KEY (id_local) REFERENCES dim_local(id_local),
    FOREIGN KEY (id_causa) REFERENCES dim_causa(id_causa),
    FOREIGN KEY (id_tipo) REFERENCES dim_tipo(id_tipo),
    FOREIGN KEY (id_classificacao) REFERENCES dim_classificacao(id_classificacao),
    FOREIGN KEY (id_fase) REFERENCES dim_fase_dia(id_fase),
    FOREIGN KEY (id_clima) REFERENCES dim_clima(id_clima),
    FOREIGN KEY (id_pista) REFERENCES dim_pista(id_pista),
    FOREIGN KEY (id_tracado) REFERENCES dim_tracado(id_tracado)
);

------------------------------------------------------------
-- 3. INSERÇÃO DAS DIMENSÕES (DISTINCT DO CSV IMPORTADO)
--    ASSUME TABELA: stg_acidentes (STAGING)
------------------------------------------------------------

-- DIM_DATA
INSERT INTO dim_data (ano, mes, dia, data_completa)
SELECT DISTINCT ano, mes, dia, DATE(data_completa)
FROM stg_acidentes;

-- DIM_LOCAL
INSERT INTO dim_local (uf, municipio)
SELECT DISTINCT uf, municipio
FROM stg_acidentes
WHERE uf IS NOT NULL
  AND municipio IS NOT NULL;

-- DIM_CAUSA
INSERT INTO dim_causa (causa_acidente)
SELECT DISTINCT causa_acidente
FROM stg_acidentes;

-- DIM_TIPO
INSERT INTO dim_tipo (tipo_acidente)
SELECT DISTINCT tipo_acidente
FROM stg_acidentes;

-- DIM_CLASSIFICACAO
INSERT INTO dim_classificacao (classificacao_acidente)
SELECT DISTINCT classificacao_acidente
FROM stg_acidentes;

-- DIM_FASE_DIA
INSERT INTO dim_fase_dia (fase_dia)
SELECT DISTINCT fase_dia
FROM stg_acidentes;

-- DIM_CLIMA
INSERT INTO dim_clima (condicao_metereologica)
SELECT DISTINCT condicao_metereologica
FROM stg_acidentes;

-- DIM_PISTA
INSERT INTO dim_pista (tipo_pista)
SELECT DISTINCT tipo_pista
FROM stg_acidentes;

-- DIM_TRACADO
INSERT INTO dim_tracado (tracado_via)
SELECT DISTINCT tracado_via
FROM stg_acidentes;

------------------------------------------------------------
-- 4. INSERÇÃO DA TABELA FATO (JOIN DIMENSÕES)
------------------------------------------------------------

INSERT INTO fato_acidente (
    id, id_data, id_local, id_causa, id_tipo, id_classificacao,
    id_fase, id_clima, id_pista, id_tracado,
    ilesos, feridos_leves, feridos_graves, mortos,
    total_vitimas, quantidade_acidentes
)
SELECT
    s.id,
    d.id_data,
    l.id_local,
    c.id_causa,
    t.id_tipo,
    cl.id_classificacao,
    f.id_fase,
    cm.id_clima,
    p.id_pista,
    tr.id_tracado,
    s.ilesos,
    s.feridos_leves,
    s.feridos_graves,
    s.mortos,
    s.total_vitimas,
    s.quantidade_acidentes
FROM stg_acidentes s
JOIN dim_data d 
        ON d.ano = s.ano 
       AND d.mes = s.mes 
       AND d.dia = s.dia
JOIN dim_local l 
        ON l.uf = s.uf 
       AND l.municipio = s.municipio
JOIN dim_causa c 
        ON c.causa_acidente = s.causa_acidente
JOIN dim_tipo t 
        ON t.tipo_acidente = s.tipo_acidente
JOIN dim_classificacao cl 
        ON cl.classificacao_acidente = s.classificacao_acidente
JOIN dim_fase_dia f 
        ON f.fase_dia = s.fase_dia
JOIN dim_clima cm 
        ON cm.condicao_metereologica = s.condicao_metereologica
JOIN dim_pista p 
        ON p.tipo_pista = s.tipo_pista
JOIN dim_tracado tr 
        ON tr.tracado_via = s.tracado_via;

------------------------------------------------------------
-- 5. VALIDAÇÃO FINAL
------------------------------------------------------------

SELECT 
    (SELECT COUNT(*) FROM stg_acidentes) AS total_staging,
    (SELECT COUNT(*) FROM fato_acidente) AS total_fato;
