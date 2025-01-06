"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data1():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return

import os
import zipfile
import pandas as pd

def clean_campaign_data():
    # Define las rutas y columnas
    input_folder = "files/input"
    output_folder = "files/output"
    os.makedirs(output_folder, exist_ok=True)

    client_columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    campaign_columns = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_day",
        "month",
        "day",
    ]
    economics_columns = ["client_id", "cons_price_idx", "euribor_three_months"]

    # Archivos de salida
    client_file = os.path.join(output_folder, "client.csv")
    campaign_file = os.path.join(output_folder, "campaign.csv")
    economics_file = os.path.join(output_folder, "economics.csv")

    # Inicializa DataFrames
    client_df = pd.DataFrame()
    campaign_df = pd.DataFrame()
    economics_df = pd.DataFrame()

    # Procesa cada archivo ZIP
    def process_zip_file(zip_path):
        nonlocal client_df, campaign_df, economics_df
        with zipfile.ZipFile(zip_path, "r") as z:
            for file_name in z.namelist():
                if file_name.endswith(".csv"):
                    with z.open(file_name) as file:
                        df = pd.read_csv(file)

                        # Filtrar columnas y agregar a los DataFrames correspondientes
                        if any(col in df.columns for col in client_columns):
                            client_df = pd.concat(
                                [client_df, df[[col for col in client_columns if col in df.columns]]],
                                ignore_index=True
                            )
                        if any(col in df.columns for col in campaign_columns):
                            campaign_df = pd.concat(
                                [campaign_df, df[[col for col in campaign_columns if col in df.columns]]],
                                ignore_index=True
                            )
                        if any(col in df.columns for col in economics_columns):
                            economics_df = pd.concat(
                                [economics_df, df[[col for col in economics_columns if col in df.columns]]],
                                ignore_index=True
                            )

    # Recorre la carpeta de entrada
    for file in os.listdir(input_folder):
        if file.endswith(".zip"):
            process_zip_file(os.path.join(input_folder, file))

    # Guarda los archivos CSV
    client_df.to_csv(client_file, index=False)
    campaign_df.to_csv(campaign_file, index=False)
    economics_df.to_csv(economics_file, index=False)

    print("Archivos procesados y guardados en la carpeta de salida.")

    #Limpiar clientes

    # Leer el archivo CSV
    df = pd.read_csv('files/output/client.csv')

    # Transformar la columna 'job'
    df['job'] = df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)

    # Transformar la columna 'education'
    df['education'] = df['education'].str.replace('.', '_', regex=False)
    df['education'] = df['education'].replace('unknown', pd.NA)

    # Transformar la columna 'credit_default'
    df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)

    # Transformar la columna 'mortgage'
    df['mortgage'] = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    # Guardar el archivo CSV con los cambios
    df.to_csv('files/output/client.csv', index=False)

    #limpiar campaign

    # Leer el archivo CSV
    df = pd.read_csv('files/output/campaign.csv')

    # Transformar la columna 'previous_outcome'
    df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)

    # Transformar la columna 'campaign_outcome'
    df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    # Convertir las columnas 'month' y 'day' a enteros
    df['month'] = pd.to_datetime(df['month'], format='%b').dt.month
    df['day'] = pd.to_numeric(df['day'], errors='coerce')

    # Crear la columna 'last_contact_day' combinando 'day' y 'month' con el año 2022
    df['last_contact_day'] = df.apply(
        lambda row: f"2022-{int(row['month']):02d}-{int(row['day']):02d}" 
        if pd.notna(row['month']) and pd.notna(row['day']) else '2022-01-01', axis=1
    )
    # Eliminar las columnas 'month' y 'day'
    df = df.drop(['month', 'day'], axis=1)


    # Guardar el archivo CSV con los cambios
    df.to_csv('files/output/campaign.csv', index=False)

    print("Transformaciones completadas y guardadas en el archivo.")




if __name__ == "__main__":
    clean_campaign_data()


