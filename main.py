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

        print("Execução concluída com sucesso")
        return "Pipeline executado com sucesso!", 200

    except Exception as e:
        print("Erro durante execução:", str(e))
        import traceback
        traceback.print_exc()
        return f"Erro durante execução: {str(e)}", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
