import os
import re
import zipfile
import shutil
from pyspark.sql.functions import col, concat
import requests


class ReceitaPipeline:
    """
    Classe responsável por gerenciar o pipeline de download, extração, tratamento e integração
    de dados da Receita Federal.
    """

    def __init__(self, staging, raw, curated, sandbox, path_rfb):
        """
        Inicializa os diretórios e configura o SparkSession.

        Args:
            staging (str): Caminho para a camada de staging.
            raw (str): Caminho para a camada raw.
            curated (str): Caminho para a camada curated.
            sandbox (str): Caminho para a camada sandbox.
            path_rfb (str): Caminho relativo para os dados da Receita.
        """

        self.staging = staging
        self.raw = raw
        self.curated = curated
        self.sandbox = sandbox
        self.path_rfb = path_rfb

        # Configura caminhos completos para as camadas
        self.rfb_stag = os.path.join(staging, path_rfb)
        self.rfb_raw = os.path.join(raw, path_rfb)
        self.rfb_cur = os.path.join(curated, path_rfb)
        self.rfb_sand = os.path.join(sandbox, path_rfb)

    @staticmethod
    def delete_old_versions(paths, current_version):
        """
        Remove arquivos de versões antigas com base na versão atual.

        Args:
            paths (list): Lista de caminhos dos arquivos.
            current_version (str): Versão atual que deve ser preservada.
        """

        files_to_delete = [
            file for file in paths if not re.search(current_version, file)
        ]
        for file in files_to_delete:
            try:
                os.remove("/dbfs" + file)
            except:
                shutil.rmtree("/dbfs" + file)

    def correct_corrupted_files(self, file):
        """
        Corrige arquivos corrompidos, baixando uma nova cópia do host.

        Args:
            file (str): Caminho do arquivo corrompido.
        """

        url_base = "http://200.152.38.155/CNPJ/"
        file_name = file.split("/")[-1][:-4]

        try:
            # Verifica a integridade do arquivo
            with zipfile.ZipFile("/dbfs" + file, "r") as zip_ref:
                zip_ref.extractall("/dbfs/tmp/")
            os.remove("/dbfs/tmp/" + file_name)
            print(f"File {file_name} is not corrupted.")
        except:
            print(f"File {file_name} is corrupted. Downloading a new copy.")
            response = requests.get(url_base + file_name + ".zip")
            # Baixa e substitui o arquivo corrompido
            with open("/dbfs" + file, "wb") as output_file:
                output_file.write(response.content)
            print(f"File {file_name} downloaded and saved.")

    def extract_files(self):
        """
        Extrai arquivos zipados da camada staging para a camada raw.
        """

        paths = [
            os.path.join(self.rfb_stag, x) for x in os.listdir("/dbfs" + self.rfb_stag)
        ]
        for file in paths:
            with zipfile.ZipFile("/dbfs" + file, "r") as zip_ref:
                file_type = file.split(".")[-2]
                if file_type == "EMPRECSV":
                    zip_ref.extractall("/dbfs" + self.rfb_raw + "EMPRESA/")
                elif file_type == "ESTABELE":
                    zip_ref.extractall("/dbfs" + self.rfb_raw + "ESTABELECIMENTOS/")
                elif file_type == "SOCIOCSV":
                    zip_ref.extractall("/dbfs" + self.rfb_raw + "SOCIOS/")
                else:
                    zip_ref.extractall("/dbfs" + self.rfb_raw + "SUPORTE/")

    def transform_to_curated(self, dict_cols, s_acao):
        """
        Transforma e move os dados da camada raw para a camada curated.

        Args:
            dict_cols (dict): Mapeamento de tipos de arquivos para colunas.
            s_acao (list): Lista de ações específicas.
        """

        dirs = ["EMPRESA", "ESTABELECIMENTOS", "SOCIOS", "SUPORTE"]

        for dir_ in dirs:
            raw_dir = os.path.join(self.rfb_raw, dir_)
            paths = [os.path.join(raw_dir, x) for x in os.listdir("/dbfs" + raw_dir)]
            self.delete_old_versions(paths, self.get_current_version(paths))

            for file in paths:
                file_type = file.split(".")[-1]
                columns = (
                    dict_cols.get(file_type) or dict_cols.get("SIMPLES")
                    if file_type not in s_acao
                    else None
                )
                if columns:
                    # Lê e transforma os dados
                    df = (
                        self.spark.read.options(delimiter=";")
                        .csv("/dbfs" + file)
                        .toDF(*columns)
                    )
                    df.write.mode("overwrite").parquet(
                        os.path.join(self.rfb_cur, dir_, os.path.basename(file))
                    )

            # Remove versões antigas da camada curated
            curated_dir = os.path.join(self.rfb_cur, dir_)
            curated_paths = [
                os.path.join(curated_dir, x) for x in os.listdir("/dbfs" + curated_dir)
            ]
            self.delete_old_versions(
                curated_paths, self.get_current_version(curated_paths)
            )

    @staticmethod
    def get_current_version(paths):
        """
        Determina a versão mais recente com base nos nomes dos arquivos.

        Args:
            paths (list): Lista de caminhos dos arquivos.

        Returns:
            str: Versão mais recente.
        """
        versions = {item.split(".")[-3] for item in paths if "CSV" not in item}
        return max(versions)

    def stack_data(self):
        """
        Empilha os dados da camada curated em arquivos únicos por tipo.
        """
        dirs = ["EMPRESA", "ESTABELECIMENTOS", "SOCIOS"]

        for dir_ in dirs:
            curated_dir = os.path.join(self.rfb_cur, dir_)
            paths = [
                os.path.join(curated_dir, x) for x in os.listdir("/dbfs" + curated_dir)
            ]

            # Empilha os dados
            stacked_df = self.spark.read.parquet(paths[0])

            for path in paths[1:]:
                stacked_df = stacked_df.union(self.spark.read.parquet(path))

            stacked_df.write.mode("overwrite").parquet(
                self.rfb_sand + f"{dir_}_EMPILHADO_temp.parquet"
            )

    def join_data(self):
        """
        Realiza o join dos dados empilhados das diferentes camadas.

        Returns:
            DataFrame: DataFrame resultante do join.
        """
        empresas = self.spark.read.parquet(
            self.rfb_sand + "EMPRESA_EMPILHADO_temp.parquet"
        )
        estabelecimentos = self.spark.read.parquet(
            self.rfb_sand + "ESTABELECIMENTOS_EMPILHADO_temp.parquet"
        )
        socios = self.spark.read.parquet(
            self.rfb_sand + "SOCIOS_EMPILHADO_temp.parquet"
        ).drop("PAIS")

        simples_path = [
            os.path.join(self.rfb_cur, "SUPORTE", x)
            for x in os.listdir("/dbfs" + self.rfb_cur + "SUPORTE")
            if re.search("SIMPLES", x)
        ][0]
        simples = self.spark.read.parquet(simples_path)

        merged_df = (
            estabelecimentos.join(empresas, ["CNPJ_BASICO"], "left")
            .join(simples, ["CNPJ_BASICO"], "left")
            .join(socios, ["CNPJ_BASICO"], "left")
        )

        return merged_df.withColumn(
            "CNPJ_FULL", concat(col("CNPJ_BASICO"), col("CNPJ_ORDEM"), col("CNPJ_DV"))
        )

    def save_results(self, df, suffix):
        """
        Salva os resultados em formatos Parquet e Delta.

        Args:
            df (DataFrame): DataFrame a ser salvo.
            suffix (str): Sufixo para os nomes dos arquivos de saída.
        """

        # Salva em Parquet
        df.write.mode("overwrite").parquet(
            self.rfb_sand + f"ESTABELECIMENTOS_EMPRESAS_{suffix}"
        )
        # Salva em Delta
        df.write.format("delta").mode("overwrite").save(
            self.sandbox + f"Delta/ESTABELECIMENTOS_EMPRESAS_{suffix}.delta"
        )
