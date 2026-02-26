-- Criação da Dimensão IES (Instituições de Ensino Superior)
CREATE TABLE dim_ies (
    co_ies NUMERIC(4) PRIMARY KEY, -- CO_IES
    no_ies VARCHAR(255) NOT NULL   -- Nome da IES
);

-- Criação da Dimensão Município (Filtro UF=35, São Paulo)
CREATE TABLE dim_municipio (
    co_municipio NUMERIC(7) PRIMARY KEY, -- CO_MUNICIPIO (Código Município Completo)
    no_municipio VARCHAR(255) NOT NULL   -- Nome do Município
);

-- Criação da Tabela Fato Matrículas
CREATE TABLE fato_matriculas (
    -- Chaves Estrangeiras (Integram o Data Mart)
    fk_co_ies NUMERIC(4) REFERENCES dim_ies (co_ies),
    fk_co_municipio NUMERIC(7) REFERENCES dim_municipio (co_municipio),

    -- Atributos de Dimensão Redundantes (para Análise)
    sexo VARCHAR(9),
    grau_academico VARCHAR(12),
    rede VARCHAR(10),
    modalidade VARCHAR(12),
    ano NUMERIC(4),

    -- Métrica (Fato)
    qt_matricula NUMERIC(10) NOT NULL
);

-- Criação de Chave Primária Composta 
ALTER TABLE fato_matriculas
ADD PRIMARY KEY (fk_co_ies, fk_co_municipio, sexo, grau_academico, rede, modalidade, ano);