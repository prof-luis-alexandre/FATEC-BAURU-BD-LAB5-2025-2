SELECT
    f.ano,
    f.qt_matricula,
    d_ies.no_ies,
    d_mun.no_municipio,
    f.sexo,
    f.grau_academico,
    f.rede,
    f.modalidade
FROM
    fato_matriculas f
JOIN
    dim_ies d_ies ON f.fk_co_ies = d_ies.co_ies
JOIN
    dim_municipio d_mun ON f.fk_co_municipio = d_mun.co_municipio;