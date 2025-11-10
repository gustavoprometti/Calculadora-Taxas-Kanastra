WITH faixas_com_limites AS (
    SELECT 
        fv.cliente,
        empresa,
        `fund id` as fund_id,
        fv.faixa AS limite_inferior,
        LEAD(fv.faixa) OVER (PARTITION BY fv.cliente, fv.servico ORDER BY fv.faixa) AS limite_superior,
        fv.fee_variavel, 
        fv.servico
    FROM finance.fee_variavel fv
)
, faixas_com_limites_minimo AS ( 
    SELECT 
        fm.cliente,
        empresa,
        `fund id` as fund_id,
        fm.faixa AS limite_inferior,
        LEAD(fm.faixa) OVER (PARTITION BY fm.cliente, fm.servico ORDER BY fm.faixa) AS limite_superior,
        fm.fee_min, 
        fm.servico
    FROM finance.fee_minimo fm 
),
soma_diaria_pl AS (
    SELECT
        q.fund_id,
        i.name,
        q.reference_dt AS dia,
        SUM(q.net_worth) AS pl_total_diario
    FROM hub.funds i 
    LEFT JOIN 
    ( SELECT fund_id, reference_dt, net_worth
    from investment.quotas WHERE  fund_id NOT in (302,76)
    UNION ALL
    select 
fund_id,
reference_dt,
sum(market_value) as pl
from investment.wallet
where external_id = 11301
and fund_id in (302,76)
group by 1,2
    )
    q ON i.id = q.fund_id 
    GROUP BY q.fund_id, q.reference_dt, i.name
)
,faixa_variavel AS (
    SELECT 
        s.fund_id,
        s.pl_total_diario,
        empresa,
        s.dia,
        fa.fee_variavel,
        fa.servico AS tipo_servico,
        SUM(
            CASE 
                WHEN fa.limite_superior IS NULL THEN GREATEST(0, s.pl_total_diario - fa.limite_inferior)
                ELSE GREATEST(0, LEAST(s.pl_total_diario, fa.limite_superior) - fa.limite_inferior)
            END
        ) AS valor_faixa_variavel
    FROM soma_diaria_pl s
    LEFT JOIN faixas_com_limites fa ON fa.fund_id  = s.fund_id
     where s.fund_id <> 150
    GROUP BY s.fund_id, s.dia, fa.servico, s.pl_total_diario, fa.fee_variavel,empresa
    
UNION ALL
-- calculo específico pro fund id 150
    SELECT 
        s.fund_id,
        s.pl_total_diario,
        empresa,
        s.dia,
        fm.fee_variavel,
        fm.servico AS tipo_servico,
        s.pl_total_diario AS valor_faixa_minimo
FROM soma_diaria_pl s 
LEFT JOIN faixas_com_limites fm 
    ON  fm.fund_id  = s.fund_id
    AND s.pl_total_diario BETWEEN fm.limite_inferior 
                              AND COALESCE(fm.limite_superior, s.pl_total_diario)
    where s.fund_id = 150
    GROUP BY s.fund_id, s.dia, fm.servico, s.pl_total_diario, fm.fee_variavel, empresa   
)

,faixa_minimo AS (
    SELECT 
        s.fund_id,
        s.pl_total_diario,
        empresa,
        s.dia,
        fm.fee_min,
        fm.servico AS tipo_servico,
        s.pl_total_diario AS valor_faixa_minimo
    FROM soma_diaria_pl s
LEFT JOIN faixas_com_limites_minimo fm 
    ON s.fund_id = fm.fund_id
    AND s.pl_total_diario BETWEEN fm.limite_inferior 
                              AND COALESCE(fm.limite_superior, s.pl_total_diario)
    -- where s.fund_id = 88
    GROUP BY s.fund_id, s.dia, fm.servico, s.pl_total_diario, fm.fee_min, empresa    
    -- order by  s.dia desc
),
BusinessDaysCount AS (
    SELECT
        DATE_TRUNC(DATE, MONTH) AS month_start,
        COUNT(*) AS business_days_in_month
    FROM investment.calendar
    WHERE is_business_day_br IS TRUE
    AND DATE >= DATE('2024-01-01')
    AND DATE <= DATE('2026-01-01') 
    GROUP BY DATE_TRUNC(DATE, MONTH)
),
dias_uteis AS (
    SELECT
        c.date,

        b.business_days_in_month
    FROM investment.calendar c
    JOIN BusinessDaysCount b ON DATE_TRUNC(DATE, MONTH) = b.month_start
    WHERE c.is_business_day_br IS TRUE
    AND c.DATE >= DATE('2024-01-01') 
    AND c.DATE <= DATE('2026-01-01')
)
, taxas AS (
SELECT 
    fv.fund_id AS id_fundo,
    'variavel' as tipo,
    empresa,
    dia,
    CASE WHEN tipo_servico LIKE '%Admini%' THEN 'Administração' ELSE tipo_servico END AS tipo_servico,
    SUM(fee_variavel / 252 * valor_faixa_variavel) AS taxa
FROM   faixa_variavel fv
INNER JOIN dias_uteis du ON du.date = dia
-- where fund_id = 88
-- and fv.dia = '2024-12-18'
GROUP BY 1,2,3,4,5

UNION ALL
 
SELECT 
    fm.fund_id AS id_fundo,
    'minimo' as tipo,
    empresa,
    dia,
    CASE WHEN tipo_servico LIKE '%Admini%' THEN 'Administração' ELSE tipo_servico END AS tipo_servico,
    SUM(valor_faixa_minimo * fee_min / pl_total_diario ) AS taxa
FROM   faixa_minimo fm 
INNER JOIN dias_uteis du ON du.date = dia
where valor_faixa_minimo > 0
-- where fund_id = 88
-- and fm.dia = '2024-12-18'
GROUP BY 1,2,3,4,5
-- order by dia desc
)
, quotas AS (
SELECT 
    fund_id, 
    reference_dt, 
    SUM(net_worth) AS pl, 
    ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY reference_dt desc) AS seq
FROM     ( SELECT fund_id, reference_dt, net_worth
    from investment.quotas WHERE  fund_id NOT in (302,76)
    UNION ALL
    select 
fund_id,
reference_dt,
sum(market_value) as pl
from investment.wallet
where external_id = 11301
and fund_id in (302,76)
group by 1,2
    )
where net_worth > 0
GROUP BY fund_id, reference_dt

)

, x_variavel AS ( 
SELECT 
    dia AS date_ref,
    id_fundo AS fund_id,
    ROW_NUMBER() OVER (PARTITION BY id_fundo ORDER BY dia desc) AS seq1,
    MAX(empresa) empresa,
    f.name AS fund_name,
    government_id AS cnpj,
    net_worth,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Administração' THEN taxa * 252 / net_worth ELSE NULL END) AS taxa_variavel_adm,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Administração' THEN taxa * business_days_in_month ELSE NULL END) AS fee_min_adm,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Administração' THEN taxa  ELSE NULL END) AS fee_variavel_diario_adm,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Administração' THEN taxa  ELSE NULL END) AS fee_min_diario_adm,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Gestão' THEN taxa * 252 / net_worth ELSE NULL END) AS taxa_variavel_gestao,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Gestão' THEN taxa * business_days_in_month ELSE NULL END) AS fee_min_gestao,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Gestão' THEN taxa  ELSE NULL END) AS fee_variavel_diario_gestao,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Gestão' THEN taxa  ELSE NULL END) AS fee_min_diario_gestao,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Custódia' THEN taxa * 252 / net_worth ELSE NULL END) AS taxa_variavel_custodia,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia' THEN taxa * business_days_in_month ELSE NULL END) AS fee_min_custodia,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Custódia' THEN taxa  ELSE NULL END) AS fee_variavel_diario_custodia,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia' THEN taxa  ELSE NULL END) AS fee_min_diario_custodia,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Custódia Kanastra' THEN taxa * 252 / net_worth ELSE NULL END) AS taxa_variavel_consultoria,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia Kanastra' THEN taxa * business_days_in_month ELSE NULL END) AS fee_min_consultoria,
    MAX(CASE WHEN tipo = 'variavel' AND tipo_servico = 'Custódia Kanastra' THEN taxa  ELSE NULL END) AS fee_variavel_diario_consultoria,
    -- MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia Kanastra' THEN taxa  ELSE NULL END) AS fee_min_diario_consultoria,
FROM taxas 
INNER JOIN hub.funds f ON id_fundo = id
INNER JOIN dias_uteis du ON du.date = dia
LEFT JOIN (SELECT fund_id, reference_dt, sum(net_worth) net_worth from     ( SELECT fund_id, reference_dt, net_worth
    from investment.quotas WHERE  fund_id NOT in (302,76)
    UNION ALL
    select 
fund_id,
reference_dt,
sum(market_value) as pl
from investment.wallet
where external_id = 11301
and fund_id in (302,76)
group by 1,2
    ) where net_worth > 0 group by 1,2) q ON q.fund_id = id_fundo and  dia = reference_dt
where net_worth > 0
GROUP BY dia, id_fundo, f.name, government_id, net_worth
)

, x_minimo AS ( 
SELECT 
    dia AS date_ref,
    id_fundo AS fund_id,
    ROW_NUMBER() OVER (PARTITION BY id_fundo ORDER BY dia desc) AS seq1,
    MAX(empresa) empresa,
    f.name AS fund_name,
    government_id AS cnpj,
    net_worth,
MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Administração' 
    THEN taxa  * business_days_in_month 
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Administração' 
    THEN taxa 
    ELSE NULL 
END) AS fee_min_adm, 

MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Administração' 
    THEN taxa
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Administração' 
    THEN taxa 
    ELSE NULL 
END) AS fee_min_diario_adm, 

MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Custódia' 
    THEN taxa  * business_days_in_month 
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Custódia' 
    THEN taxa  
    ELSE NULL 
END) AS fee_min_custodia, 

MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Custódia' 
    THEN taxa
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Custódia' 
    THEN taxa 
    ELSE NULL 
END) AS fee_min_diario_custodia, 

MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Gestão' 
    THEN taxa  * business_days_in_month 
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Gestão' 
    THEN taxa 
    ELSE NULL 
END) AS fee_min_gestao, 

MAX(CASE 
    WHEN id_fundo IS NOT NULL AND tipo = 'minimo' AND tipo_servico = 'Gestão' 
    THEN taxa 
    WHEN id_fundo IS NULL AND tipo = 'minimo' AND tipo_servico = 'Gestão' 
    THEN taxa 
    ELSE NULL 
END) AS fee_min_diario_gestao, 


    MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia Kanastra' THEN taxa * business_days_in_month ELSE NULL END) AS fee_min_consultoria,

    MAX(CASE WHEN tipo = 'minimo' AND tipo_servico = 'Custódia Kanastra' THEN taxa  ELSE NULL END) AS fee_min_diario_consultoria,
FROM taxas 
INNER JOIN hub.funds f ON id_fundo = id
INNER JOIN dias_uteis du ON du.date = dia
LEFT JOIN (SELECT fund_id, reference_dt, sum(net_worth) net_worth from     ( SELECT fund_id, reference_dt, net_worth
    from investment.quotas WHERE  fund_id NOT in (302,76)
    UNION ALL
    select 
fund_id,
reference_dt,
sum(market_value) as pl
from investment.wallet
where external_id = 11301
and fund_id in (302,76)
group by 1,2
    )group by 1,2) q ON q.fund_id = id_fundo and  dia = reference_dt
-- LEFT JOIN correcoes c ON id_fundo = id_fundo  

GROUP BY dia, id_fundo, f.name, government_id, net_worth, business_days_in_month
)
, ufa2 AS (
SELECT 
    q.reference_dt,
    x.date_ref,
    q.fund_id,
    x.empresa,
    x.fund_name,
    x.cnpj,
    x.net_worth,
    business_days_in_month,
    taxa_variavel_adm,
    fee_min_diario_adm as fee_min_adm,
    fee_variavel_diario_adm ,
    fee_min_diario_adm / business_days_in_month as fee_min_diario_adm,
    taxa_variavel_gestao,
    fee_min_diario_gestao as fee_min_gestao,
    fee_variavel_diario_gestao,
    fee_min_diario_gestao / business_days_in_month as fee_min_diario_gestao,
    taxa_variavel_custodia,
    fee_min_diario_custodia  as fee_min_custodia,
    fee_variavel_diario_custodia, 
    fee_min_diario_custodia / business_days_in_month as fee_min_diario_custodia,
    taxa_variavel_consultoria,
    fee_min_diario_consultoria as fee_min_consultoria,
    fee_variavel_diario_consultoria,
    fee_min_diario_consultoria / business_days_in_month as fee_min_diario_consultoria
FROM 
    quotas q 
LEFT JOIN 
    x_variavel x 
    ON q.fund_id = x.fund_id 
    AND (
        (q.fund_id IN (41, 6, 62, 40, 36, 98, 96, 161,  178, 187, 232, 247,245, 164, 268,179,295, 274,322, 291) AND seq = x.seq1)
        OR (q.fund_id NOT IN (41, 6, 62, 40, 36, 98, 96, 161, 178, 187, 232, 247,245, 164, 268,179,295, 274,322,291) AND seq = x.seq1 - 1)
    )
LEFT JOIN 
    x_minimo xm ON  q.fund_id = xm.fund_id
    AND (
        (q.fund_id IN (41, 6, 62, 40, 36, 98, 96, 161,  178, 187, 232, 247,245, 164, 268,179,295, 274,322,291) AND seq = xm.seq1)
        OR (q.fund_id NOT IN (41, 6, 62, 40, 36, 98, 96, 161, 178, 187, 232, 247,245, 164, 268,179,295, 274,322,291) AND seq = xm.seq1 - 1)
    )
LEFT JOIN dias_uteis du on du.date =  q.reference_dt

-- where q.fund_id = 88



  )
, dt_geral AS ( 
    SELECT 
        f.id AS fund_id, 
        d.date
    FROM 
        kanastra-live.hub.funds AS f
    CROSS JOIN 
        dias_uteis AS d
)

, indices AS (
SELECT
    f.fund_id, 
    DATE_TRUNC(f.inicio_fundo, month) AS data_inicio,
    i.ref_date,
    DATE_DIFF(i.ref_date, DATE_TRUNC(f.inicio_fundo, month), MONTH) AS dif_meses,
    DIV(DATE_DIFF(i.ref_date, DATE_TRUNC(f.inicio_fundo, month), MONTH), 12) * 12 AS mes_corretor,
    i.igpm,
    CASE 
        WHEN f.indice_correcao = 'igpm' 
            THEN EXP(SUM(LOG(1 + COALESCE(i.igpm, 0) / 100)) 
                     OVER (PARTITION BY f.fund_id, DIV(DATE_DIFF(i.ref_date, DATE_TRUNC( DATE_ADD(f.inicio_fundo, INTERVAL 1 MONTH) , month), MONTH), 12)
                           ORDER BY i.ref_date))
        WHEN f.indice_correcao = 'ipca' 
            THEN EXP(SUM(LOG(1 + COALESCE(i.ipca, 0) / 100)) 
                     OVER (PARTITION BY f.fund_id, DIV(DATE_DIFF(i.ref_date, DATE_TRUNC( DATE_ADD(f.inicio_fundo, INTERVAL 1 MONTH) , month), MONTH), 12)
                           ORDER BY i.ref_date))
        WHEN f.indice_correcao = 'ipc_fipe' 
            THEN EXP(SUM(LOG(1 + COALESCE(i.ipc_fipe, 0) / 100)) 
                     OVER (PARTITION BY f.fund_id, DIV(DATE_DIFF(i.ref_date, DATE_TRUNC( DATE_ADD(f.inicio_fundo, INTERVAL 1 MONTH) , month), MONTH), 12)
                           ORDER BY i.ref_date))
    END AS fator_correcao_mes
FROM
    kanastra-live.finance.correcao_aux_v3 f
LEFT JOIN
    kanastra-live.finance.indices_v3 i 
        ON i.ref_date >= DATE_TRUNC( DATE_ADD(f.inicio_fundo, INTERVAL 1 MONTH) , month)
)
, fatores_marco as (

SELECT 
fund_id,
data_inicio,
ref_date,	
dif_meses,	
mes_corretor,	
CASE WHEN fator_correcao_mes < 1 THEN 1 ELSE fator_correcao_mes END AS fator_correcao_marco
FROM indices 
where dif_meses IN (12,24,36,48,60,72,84)
)
, fatores1 as (
SELECT
  i.fund_id,
  i.data_inicio,
  i.ref_date,
  i.dif_meses,
  i.mes_corretor,
  COALESCE(f.fator_correcao_marco, 1) AS fator_correcao
FROM indices i
LEFT JOIN fatores_marco f
  ON f.fund_id = i.fund_id
 AND f.mes_corretor = i.mes_corretor
-- opcional: limitar até 5 anos
-- WHERE i.dif_meses < 60
ORDER BY i.ref_date

)
, multiplicadores1 as (

  SELECT
    fund_id,
    data_inicio,
    dif_meses,
    DATE_ADD(ref_date, INTERVAL 1 MONTH) AS mes_seguinte,
    CASE WHEN fator_correcao < 1 THEN 1 ELSE fator_correcao END AS fator_correcao1
  FROM fatores1
--   WHERE fund_id = 116
)

, multiplicadores as (
SELECT
  *,
  EXP(SUM(LN(CASE WHEN MOD(dif_meses,12)=0 THEN fator_correcao1 ELSE 1 END)) 
      OVER (PARTITION BY fund_id ORDER BY dif_meses ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
     ) AS fator_correcao
FROM multiplicadores1
)


  , ufa2_completa AS (
    SELECT
        u.reference_dt AS date_ref,
        dt.fund_id,
 fund_name,   
cnpj,  
 net_worth,  
 business_days_in_month,
 taxa_variavel_adm,
 fee_min_adm * COALESCE(fator_correcao,1) as fee_min_adm,
 fee_variavel_diario_adm,
 fee_min_diario_adm * COALESCE(fator_correcao,1) as fee_min_diario_adm,
 taxa_variavel_gestao,
 fee_min_gestao * COALESCE(fator_correcao,1) as fee_min_gestao,
 fee_variavel_diario_gestao,
 fee_min_diario_gestao * COALESCE(fator_correcao,1) as fee_min_diario_gestao,
 taxa_variavel_custodia,
 fee_min_custodia * COALESCE(fator_correcao,1) as fee_min_custodia,
 fee_variavel_diario_custodia,
 fee_min_diario_custodia * COALESCE(fator_correcao,1) as fee_min_diario_custodia,
 taxa_variavel_consultoria as taxa_variavel_custodia_kanastra,
 fee_min_consultoria * COALESCE(fator_correcao,1) as  fee_min_custodia_kanastra,
 fee_variavel_diario_consultoria as fee_variavel_diario_custodia_kanastra,
 fee_min_diario_consultoria * COALESCE(fator_correcao,1) as fee_min_diario_custodia_kanastra,

        CASE 
            WHEN u.fund_name IS NULL THEN 1
            ELSE 0
        END AS is_missing
    FROM 
        dt_geral dt
    LEFT JOIN 
        ufa2 u
    ON 
        dt.date = date_ref AND dt.fund_id = u.fund_id
--  AND u.reference_dt = '2024-10-04' 
LEFT JOIN multiplicadores m on m.fund_id = dt.fund_id AND date_trunc(u.reference_dt, month) = date_trunc(mes_seguinte, month)
where u.reference_dt is not null
-- and dt.fund_id = 42
order by dt.date desc

  )
, comparacao AS (
  SELECT 
    date_ref,
    c.fund_id,
    fund_name,
    cnpj,
    net_worth,
    -- gross_up_servicos
    business_days_in_month,
    coalesce(gross_up_servicos, 'Sem Gross Up') AS `Gross Up`,
    taxa_variavel_adm,
    fee_min_adm,
    fee_variavel_diario_adm  / 
        NULLIF(1 - COALESCE(g.gross_adm, 0), 0) fee_variavel_diario_adm,
    fee_min_diario_adm  / 
        NULLIF(1 - COALESCE(g.gross_adm, 0), 0) fee_min_diario_adm,
    GREATEST(COALESCE(fee_variavel_diario_adm, 0), COALESCE(fee_min_diario_adm, 0)) / 
        NULLIF(1 - COALESCE(g.gross_adm, 0), 0) AS acumulado_adm,
    taxa_variavel_gestao,
    fee_min_gestao,
    fee_variavel_diario_gestao / 
        NULLIF(1 - COALESCE(g.gross_gestao, 0), 0) fee_variavel_diario_gestao,
    fee_min_diario_gestao / 
        NULLIF(1 - COALESCE(g.gross_gestao, 0), 0) fee_min_diario_gestao,
    GREATEST(COALESCE(fee_variavel_diario_gestao, 0), COALESCE(fee_min_diario_gestao, 0)) / 
        NULLIF(1 - COALESCE(g.gross_gestao, 0), 0)  AS acumulado_gestao,
    taxa_variavel_custodia,
    fee_min_custodia,
    fee_variavel_diario_custodia / 
        NULLIF(1 - COALESCE(g.gross_custodia, 0), 0) fee_variavel_diario_custodia,
    fee_min_diario_custodia / 
        NULLIF(1 - COALESCE(g.gross_custodia, 0), 0) fee_min_diario_custodia,
    GREATEST(COALESCE(fee_variavel_diario_custodia, 0), COALESCE(fee_min_diario_custodia, 0)) / 
        NULLIF(1 - COALESCE(g.gross_custodia, 0), 0) AS acumulado_custodia,
    taxa_variavel_custodia_kanastra,
    fee_min_custodia_kanastra,
    fee_variavel_diario_custodia_kanastra,
    fee_min_diario_custodia_kanastra,
    GREATEST(COALESCE(fee_variavel_diario_custodia_kanastra, 0), COALESCE(fee_min_diario_custodia_kanastra, 0)) AS acumulado_custodia_kanastra,
    is_missing
  FROM ufa2_completa c
  LEFT JOIN (
SELECT
    fund_id,
    MAX(CASE WHEN servico = 'Administração' THEN gross END) AS gross_adm,
    MAX(CASE WHEN servico = 'Gestão' THEN gross END) AS gross_gestao,
    MAX(CASE WHEN servico = 'Custódia' THEN gross END) AS gross_custodia,
    MAX(CASE WHEN servico = 'Custódia Kanastra' THEN gross END) AS gross_consultoria,
    STRING_AGG(serv_name, ' / ') AS gross_up_servicos
FROM (
    SELECT
        fund_id,
        servico,
        gross,
        CASE 
            WHEN servico = 'Administração' AND COALESCE(gross,0) > 0 THEN 'Adm'
            WHEN servico = 'Gestão' AND COALESCE(gross,0) > 0 THEN 'Gestão'
            WHEN servico = 'Custódia' AND COALESCE(gross,0) > 0 THEN 'Custódia'
            WHEN servico = 'Custódia Kanastra' AND COALESCE(gross,0) > 0 THEN 'Consultoria'
        END AS serv_name
    FROM kanastra-live.finance.gross_up
) t
-- WHERE serv_name IS NOT NULL
GROUP BY fund_id

) g ON c.fund_id = g.fund_id
--   where c.fund_id = 270
)

, tabela3 as (
SELECT
    date_ref,
    fund_id,
    fund_name,
    cnpj,
    net_worth,
    business_days_in_month,
    `Gross Up`,
    taxa_variavel_adm,
    fee_min_adm,
    fee_variavel_diario_adm,
    fee_min_diario_adm,

    -- acumulado diário real
    SUM(acumulado_adm) OVER (
        PARTITION BY fund_id, EXTRACT(YEAR FROM date_ref), EXTRACT(MONTH FROM date_ref)
        ORDER BY date_ref
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS acumulado_adm_mes,

    taxa_variavel_gestao,
    fee_min_gestao,
    fee_variavel_diario_gestao,
    fee_min_diario_gestao,

    SUM(acumulado_gestao) OVER (
        PARTITION BY fund_id, EXTRACT(YEAR FROM date_ref), EXTRACT(MONTH FROM date_ref)
        ORDER BY date_ref
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS acumulado_gestao_mes,

    taxa_variavel_custodia,
    fee_min_custodia,
    fee_variavel_diario_custodia,
    fee_min_diario_custodia,

    SUM(acumulado_custodia) OVER (
        PARTITION BY fund_id, EXTRACT(YEAR FROM date_ref), EXTRACT(MONTH FROM date_ref)
        ORDER BY date_ref
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS acumulado_custodia_mes,

    taxa_variavel_custodia_kanastra,
    fee_min_custodia_kanastra,
    fee_variavel_diario_custodia_kanastra,
    fee_min_diario_custodia_kanastra,

    SUM(acumulado_custodia_kanastra) OVER (
        PARTITION BY fund_id, EXTRACT(YEAR FROM date_ref), EXTRACT(MONTH FROM date_ref)
        ORDER BY date_ref
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS acumulado_custodia_kanastra_mes,

    is_missing
FROM comparacao
ORDER BY fund_id, date_ref)

, carteira as (

SELECT
    reference_dt,
    fund_id,
    MAX(CASE WHEN LOWER(external_name_group1) = 'txadm' THEN - market_value END) AS taxa_adm,
    MAX(CASE WHEN LOWER(external_name_group1) = 'txgestao' THEN - market_value END) AS taxa_gestao,
    MAX(CASE WHEN LOWER(external_name_group1) = 'txcust' THEN - market_value END) AS taxa_custodia,
    MAX(CASE WHEN LOWER(external_name_group1) like '%txcust%dc%'  THEN - market_value END) AS taxa_custodia_dc
FROM kanastra-live.investment.wallet
WHERE account_name_1 = 'passivo'
  AND account_name_2 = 'provisao'
GROUP BY reference_dt, fund_id

)
, y as (

SELECT
    date_ref,
    t.fund_id,
    fund_name,
    cnpj,
    net_worth,
    'Administração' AS Service,
    business_days_in_month,
    `Gross Up`,
    taxa_variavel_adm AS taxa_variavel,
    fee_min_adm AS fee_min,
    fee_variavel_diario_adm AS fee_variavel_diario,
    fee_min_diario_adm AS fee_min_diario,
    acumulado_adm_mes AS acumulado,
    taxa_adm as provisao_carteira,
    -- taxa_adm - acumulado_adm_mes  as diferenca,
    COALESCE(taxa_adm, 0) - COALESCE(acumulado_adm_mes, 0) AS diferenca,
    null as is_missing
FROM tabela3 t
LEFT JOIN carteira C ON c.fund_id = t.fund_id AND c.reference_dt = date_ref

UNION ALL

SELECT
    date_ref,
    t.fund_id,
    fund_name,
    cnpj,
    net_worth,
    'Gestão' AS Service,
    business_days_in_month,
    `Gross Up`,
    taxa_variavel_gestao,
    fee_min_gestao,
    fee_variavel_diario_gestao,
    fee_min_diario_gestao,
    acumulado_gestao_mes,
    taxa_gestao as provisao_carteira,
    -- taxa_gestao - acumulado_gestao_mes  as diferenca,
    COALESCE(taxa_gestao, 0) - COALESCE(acumulado_gestao_mes, 0) AS diferenca,
    null as is_missing
FROM tabela3 t
LEFT JOIN carteira C ON c.fund_id = t.fund_id AND c.reference_dt = date_ref

UNION ALL

SELECT
    date_ref,
    t.fund_id,
    fund_name,
    cnpj,
    net_worth,
    'Custódia' AS Service,
    business_days_in_month,
    `Gross Up`,
    taxa_variavel_custodia,
    fee_min_custodia,
    fee_variavel_diario_custodia,
    fee_min_diario_custodia,
    acumulado_custodia_mes,
    taxa_custodia as provisao_carteira,
    -- taxa_custodia - acumulado_custodia_mes  as diferenca,
    COALESCE(taxa_custodia, 0) - COALESCE(acumulado_custodia_mes, 0) AS diferenca,

    null as is_missing
FROM tabela3 t
LEFT JOIN carteira C ON c.fund_id = t.fund_id AND c.reference_dt = date_ref

UNION ALL

SELECT
    date_ref,
    t.fund_id,
    fund_name,
    cnpj,
    net_worth,
    'Custódia Kanastra' AS Service,
    business_days_in_month,
    `Gross Up`,
    taxa_variavel_custodia_kanastra,
    fee_min_custodia_kanastra,
    fee_variavel_diario_custodia_kanastra,
    fee_min_diario_custodia_kanastra,
    acumulado_custodia_kanastra_mes,
    taxa_custodia_dc as provisao_carteira,
    COALESCE(taxa_custodia_dc, 0) - COALESCE(acumulado_custodia_kanastra_mes, 0) AS diferenca,
--   as diferenca,
    null as is_missing
FROM tabela3 t
LEFT JOIN carteira C ON c.fund_id = t.fund_id AND c.reference_dt = date_ref
)
, sinqia as (

select distinct 
fund_id,
f.name as fund_name,
fq.type,
min (quota_external_id) quota_external_id,
fq.is_active,
from hub.funds f 
inner join kanastra-live.hub.fund_quotas fq
on fq.fund_id = f.id
where fq.type in ('Sub', 'Classe única')
and fq.is_active is true
group by 1,2,3,5
)


select y.*, f.type, quota_external_id as fund_type from y 
inner join kanastra-live.hub.funds f on f.id = fund_id
LEFT JOIN sinqia s on s.fund_id = y.fund_id

where acumulado > 0
-- and fund_id =101