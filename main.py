import os
import time
import re
import pandas as pd
import boto3
from flask import Flask
from google.cloud import bigquery

app = Flask(__name__)

def run_athena_to_bq(query, database, s3_output, bq_table):
    athena = boto3.client("athena", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    s3 = boto3.client("s3")
    bq_client = bigquery.Client(project=os.getenv("GCP_PROJECT", "asksuite-salesops"))

    print(f"→ executando query para {bq_table}...")
    execution = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output},
    )
    execution_id = execution["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        raise Exception(f"query falhou: {result['QueryExecution']['Status']}")

    output_location = result["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    match = re.match(r"s3://([^/]+)/(.+)", output_location)
    bucket, key = match.groups()

    s3_obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(s3_obj["Body"])
    # arredonda todas as colunas float para 2 casas
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(2)
    print(f"→ retornou {df.shape[0]} linhas.")

    job = bq_client.load_table_from_dataframe(
        df,
        bq_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f" tabela {bq_table} atualizada com sucesso.\n")


@app.route("/", methods=["GET"])
def main():
    print("Iniciando execução do pipeline Athena → BigQuery")

    try:
        query1 = """
        WITH w_askflow AS (
            SELECT company_id, COALESCE(
                json_extract_scalar(name_i18n, '$["pt-br"]'),
                json_extract_scalar(name_i18n, '$["en-us"]'),
                json_extract_scalar(name_i18n, '$["es-es"]'),
                json_extract_scalar(name_i18n, '$["text"]')
            ) AS name
            FROM flow_daily.public_en_organization
            CROSS JOIN UNNEST(cast(json_parse(ids_external_partner) AS array<varchar>)) AS t(company_id)
            WHERE status = 'active'
        )
        SELECT
            public_companies.company_id,
            json_extract_scalar(json, '$.whatsAppNumberGupshup') AS whatsappNumber,
            json_extract_scalar(json, '$.code7PhoneNumber') AS voipNumber,
            w_askflow.name AS askFlowName
        FROM asksuite_control.public_companies
        LEFT JOIN w_askflow ON w_askflow.company_id = public_companies.company_id
        WHERE COALESCE(json_extract_scalar(json, '$.disabled'), 'false') = 'false'
        ORDER BY public_companies.company_id
        """

        query2 = """
        SELECT
        company_id,
        count(*) AS count_reservations,
        sum(cast(total_price_brl AS double)) FILTER (WHERE user_id IS NOT NULL) AS total_human_brl_90_days,
        sum(cast(total_price_brl AS double)) FILTER (WHERE user_id IS NULL) AS total_bot_brl_90_days,
        sum(cast(total_price_brl AS double)) AS total_brl_90_days
        FROM asksuite_control.public_reservations
        WHERE reservation_date >= date_add('month', -3, current_date)
        GROUP BY company_id
        ORDER BY total_brl_90_days DESC
        """

        query3 = """
        WITH w_reservations AS (
            SELECT
                r.booking_engine,
                MIN(r.created_at) AS primeira_reserva,
                MAX(r.created_at) AS ultima_reserva,
                COUNT(*) AS count_reservas,
                COUNT(DISTINCT r.company_id) AS count_empresas,
                array_agg(DISTINCT r.company_id) AS array_empresas,
                COUNT(DISTINCT CASE
                    WHEN COALESCE(json_extract_scalar(c.json, '$.disabled'), 'false') = 'false'
                    THEN r.company_id
                END) AS count_empresas_ativas,
                SUM(CAST(r.total_price AS DOUBLE)) AS total_vendido
            FROM asksuite_control.public_reservations r
            JOIN asksuite_control.public_companies c
                ON r.company_id = c.company_id
            WHERE r.company_id NOT LIKE '%.%'
              AND r.id_pixel_event IS NOT NULL
            GROUP BY r.booking_engine
        )
        SELECT
            COALESCE(w.booking_engine, 'Não identificado') AS motor_de_reservas,
            w.primeira_reserva,
            w.ultima_reserva,
            COALESCE(w.count_reservas, 0) AS count_reservas,
            COALESCE(w.count_empresas, 0) AS count_empresas,
            w.array_empresas,
            COALESCE(w.count_empresas_ativas, 0) AS count_empresas_ativas,
            COALESCE(w.total_vendido, 0) AS total_vendido
        FROM w_reservations w
        ORDER BY 1;
        """

        query4 = """
        WITH cumulative AS (
            SELECT
                company_id,
                grouped_date,
                SUM(count_site)     OVER w AS cum_ia,
                SUM(count_whatsapp) OVER w AS cum_whatsapp,
                SUM(count_voice)    OVER w AS cum_voip
            FROM mat_daily_company_indicators
            WINDOW w AS (PARTITION BY company_id ORDER BY grouped_date)
        )
        SELECT
            company_id,
            MIN(grouped_date) FILTER (WHERE cum_ia        >= 7) AS ia_activation_date,
            MIN(grouped_date) FILTER (WHERE cum_whatsapp   >= 7) AS whatsapp_activation_date,
            MIN(grouped_date) FILTER (WHERE cum_voip       >= 7) AS voip_phone_activation_date
        FROM cumulative
        GROUP BY company_id
        ORDER BY company_id;
        """

        query5 = """
        SELECT
            c.company_id,
            app.name AS askflow_plan_name,
            ac.approved_at AS askflow_approved_at,
            ac.churned,
            ac.churned_at
        FROM sales_daily.public_contracts c
        JOIN sales_daily.public_askflow_contract ac
            ON ac.askflow_contract_id = c.askflow_contract_id
        JOIN sales_daily.public_askflow_pricing_plan app
            ON app.askflow_pricing_plan_id = ac.askflow_pricing_plan_id
        WHERE c.approved = 'true'
          AND c.churn = 'false'
          AND c.has_askflow = 'true'
          AND (ac.churned = 'false' OR ac.churned IS NULL)
        ORDER BY c.company_id;
        """

        query6 = """
        SELECT tb.company_id,
            tb.product,
            tb.activation_dt,
            json_extract_scalar(public_companies.company_contracts, '$.0.contractId') AS contract_id,
            public_contracts.business_name,
            public_contracts.company_name,
            public_contracts.cnpj,
            public_countries.country_name,
            public_countries.initials AS country_initials
        FROM (
            SELECT cast(dci.company_id AS varchar) AS company_id,
                cast('bot' AS varchar) AS product,
                cast(min(dci.grouped_date) AS varchar) AS activation_dt
            FROM asksuite_control.mat_daily_company_indicators dci
            WHERE dci.count_conversations > 1
            GROUP BY dci.company_id
            UNION ALL
            SELECT cast(dci.company_id AS varchar) AS company_id,
                cast('WhatsApp' AS varchar) AS product,
                cast(min(dci.grouped_date) AS varchar) AS activation_dt
            FROM asksuite_control.mat_daily_company_indicators dci
            JOIN asksuite_control.public_companies companies_1 ON dci.company_id = companies_1.company_id
            WHERE dci.count_whatsapp > 0 AND NULLIF(json_extract_scalar(companies_1.json, '$.whatsAppNumberGupshup'), '') IS NOT NULL
            GROUP BY dci.company_id
            UNION ALL
            SELECT cast(dci.company_id AS varchar) AS company_id,
                cast('Voip' AS varchar) AS product,
                cast(min(dci.grouped_date) AS varchar) AS activation_dt
            FROM asksuite_control.mat_daily_company_indicators dci
            WHERE dci.count_voice > 0
            GROUP BY dci.company_id
            UNION ALL
            SELECT cast(tl.company_id AS varchar) AS company_id,
                cast('Askflow' AS varchar) AS product,
                cast(date(min(tl.created_at)) AS varchar) AS activation_dt
            FROM asksuite_control.public_transmission_list tl
            WHERE tl.name = 'flow' AND tl.status = 'SENT'
            GROUP BY tl.company_id
            UNION ALL
            SELECT cast(json_extract_scalar(a.external_ids, '$.0') AS varchar) AS company_id,
                cast('WhatsApp Credits' AS varchar) AS product,
                cast(date(date_parse(split(min(w.created_at), '.')[1], '%Y-%m-%d %H:%i:%s')) AS varchar) AS activation_dt
            FROM credits_daily.public_en_wallet w
            JOIN credits_daily.public_en_account a ON a.id_account = w.id_account
            WHERE w.id_wallet_type = 2 AND w.status = 'active'
            GROUP BY json_extract_scalar(a.external_ids, '$.0')
        ) tb
        LEFT JOIN asksuite_control.public_companies ON tb.company_id = public_companies.company_id
        LEFT JOIN sales_daily.public_contracts ON public_contracts.id = cast(json_extract_scalar(public_companies.company_contracts, '$.0.contractId') AS bigint)
        LEFT JOIN sales_daily.public_countries ON public_contracts.country_id = public_countries.id
        ORDER BY activation_dt DESC
        """

        # query nova — vendas por empresa com janelas 30d e 90d
        # adaptações Athena: sem LATERAL JOIN, sem FILTER(WHERE) no COUNT/SUM
        # booking_engine da última reserva resolvido via subconsulta correlacionada no SELECT
        query7 = """
        WITH w_last_booking_engine AS (
            SELECT
                company_id,
                booking_engine
            FROM (
                SELECT
                    company_id,
                    booking_engine,
                    ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY reservation_date DESC) AS rn
                FROM asksuite_control.public_reservations
                WHERE CAST(total_price_brl AS DOUBLE) < 100000
            ) ranked
            WHERE rn = 1
        )
        SELECT
            c.company_id,
            BOOL_OR(r.id_pixel_event IS NOT NULL)                        AS tem_pixel,
            lb.booking_engine                                             AS booking_engine,

            COUNT(r.total_price_brl)                                     AS total_vendas,
            SUM(CAST(r.total_price_brl AS DOUBLE))                       AS valor_total_vendas,

            COUNT(CASE
                WHEN date_diff('day', date(r.reservation_date), current_date) <= 30
                THEN r.total_price_brl END)                              AS vendas_30d,
            SUM(CASE
                WHEN date_diff('day', date(r.reservation_date), current_date) <= 30
                THEN CAST(r.total_price_brl AS DOUBLE) ELSE 0 END)      AS receita_30d,

            COUNT(CASE
                WHEN date_diff('day', date(r.reservation_date), current_date) <= 90
                THEN r.total_price_brl END)                              AS vendas_90d,
            SUM(CASE
                WHEN date_diff('day', date(r.reservation_date), current_date) <= 90
                THEN CAST(r.total_price_brl AS DOUBLE) ELSE 0 END)      AS receita_90d

        FROM asksuite_control.public_companies c
        LEFT JOIN asksuite_control.public_reservations r
            ON r.company_id = c.company_id
            AND CAST(r.total_price_brl AS DOUBLE) < 100000
        LEFT JOIN w_last_booking_engine lb
            ON lb.company_id = c.company_id

        GROUP BY c.company_id, lb.booking_engine
        ORDER BY receita_30d DESC NULLS LAST, vendas_30d DESC, c.company_id
        """

        # query nova — canais e atendimento humano no mês do churn
        query8 = """
        WITH w_companies AS (
            SELECT
                company_id,
                CAST(churned_at AS TIMESTAMP) AS churn_requested_at
            FROM sales_daily.public_contracts
            WHERE churned_at IS NOT NULL
        ),

        w_channels AS (
            SELECT
                company_id,
                SUM(count_site)                    AS count_chatweb,
                SUM(count_whatsapp)                AS count_whatsapp,
                CASE WHEN SUM(count_site) > 0      THEN true ELSE false END AS active_chatweb,
                CASE WHEN SUM(count_whatsapp) > 0  THEN true ELSE false END AS active_whatsapp,
                CASE WHEN SUM(count_instagram) > 0 THEN true ELSE false END AS active_instagram,
                CASE WHEN SUM(count_facebook) > 0  THEN true ELSE false END AS active_facebook,
                CASE WHEN SUM(count_email) > 0     THEN true ELSE false END AS active_email,
                CASE WHEN SUM(count_voice) > 0     THEN true ELSE false END AS active_voice,
                CASE WHEN SUM(count_booking) > 0   THEN true ELSE false END AS active_booking,
                CASE WHEN SUM(count_expedia) > 0   THEN true ELSE false END AS active_expedia,
                CASE WHEN SUM(count_tiktok) > 0    THEN true ELSE false END AS active_tiktok
            FROM asksuite_control.mat_daily_company_indicators
            JOIN w_companies USING (company_id)
            WHERE CAST(grouped_date AS TIMESTAMP) >= churn_requested_at - INTERVAL '90' DAY
              AND CAST(grouped_date AS TIMESTAMP) <  churn_requested_at
            GROUP BY company_id
        ),

        w_human AS (
            SELECT
                company_id,
                COUNT(*) FILTER (WHERE has_human_attendance) AS has_human_attendance_days
            FROM (
                SELECT
                    company_id,
                    grouped_date,
                    CASE WHEN SUM(count_human) > 0 THEN true ELSE false END AS has_human_attendance
                FROM asksuite_control.mat_daily_company_indicators
                JOIN w_companies USING (company_id)
                WHERE CAST(grouped_date AS TIMESTAMP) >= churn_requested_at - INTERVAL '90' DAY
                  AND CAST(grouped_date AS TIMESTAMP) <  churn_requested_at
                GROUP BY company_id, grouped_date
            ) t0
            GROUP BY company_id
        )

        SELECT
            c.company_id,
            c.churn_requested_at,
            ch.count_chatweb,
            ch.count_whatsapp,
            ch.active_chatweb,
            ch.active_whatsapp,
            ch.active_instagram,
            ch.active_facebook,
            ch.active_email,
            ch.active_voice,
            ch.active_booking,
            ch.active_expedia,
            ch.active_tiktok,
            h.has_human_attendance_days
        FROM w_companies c
        LEFT JOIN w_channels ch ON c.company_id = ch.company_id
        LEFT JOIN w_human   h ON c.company_id = h.company_id 
        """

        run_athena_to_bq(
            query1, "datalake",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.company_id_by_products"
        )
        run_athena_to_bq(
            query2, "datalake",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.Reservations_90d_by_Company"
        )
        run_athena_to_bq(
            query3, "asksuite_control",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.motores_de_reserva_com_pixel_homologado"
        )
        run_athena_to_bq(
            query4, "asksuite_control",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.company_activation_dates"
        )
        run_athena_to_bq(
            query5, "sales_daily",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Contracts.askflow_contracts"
        )
        run_athena_to_bq(
            query6, "asksuite_control",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.mat_activation_dates_per_product"
        )
        run_athena_to_bq(
            query7, "asksuite_control",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.vendas_por_empresa"
        )
        run_athena_to_bq(
            query8, "asksuite_control",
            "s3://asksuite-athena-results/athena-temp/",
            "asksuite-salesops.Silver.churn_canais_e_atendimento"
        )

        print("Execução concluída com sucesso")
        return "Pipeline executado com sucesso!", 200

    except Exception as e:
        print("Erro durante execução:", str(e))
        import traceback
        traceback.print_exc()
        return f"Erro durante execução: {str(e)}", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))