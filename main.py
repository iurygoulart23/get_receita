from src.pipeline import ReceitaPipeline

def main():    
    
    pipeline = ReceitaPipeline(
        staging='/mnt/ADLS_Staging_datalakesenai/',
        raw='/mnt/ADLS_raw_datalakesenai/',
        curated='/mnt/ADLS_curated_datalakesenai/',
        sandbox='/mnt/ADLS_Sandbox_datalakesenai/',
        path_rfb='Dados_Externos/Bases Cadastrais - Empresas/RFB - Base de CNPJs/'
    )

    pipeline.extract_files()
    pipeline.stack_data()
    result = pipeline.join_data()
    pipeline.save_results(result, 'FINAL')

    print("Processo finalizado!")


if __name__ == "__main__":
    main()
