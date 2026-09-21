SELECT
    f.currency_code,
    p.partner_name,
    f.total_debits,
    f.total_credits,
    f.net_position,
    ROW_NUMBER() OVER (
        PARTITION BY f.currency_code
        ORDER BY f.net_position DESC, f.partner_id
    ) AS position_ranking
FROM {{ ref('fct_partner_net_position') }} AS f
INNER JOIN {{ ref('dim_partners') }} AS p
    ON f.partner_id = p.partner_id
WHERE f.net_position > 0
QUALIFY position_ranking <= 3
ORDER BY f.currency_code, position_ranking
