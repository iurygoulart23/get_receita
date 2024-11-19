# Receita Pipeline

## Descrição

O **Receita Pipeline** é um projeto que automatiza o processo de download, extração, tratamento e integração de bases de dados disponibilizadas pela Receita Federal. Ele foi projetado para lidar com instabilidades no host, garantindo a integridade e consistência dos dados, mesmo em situações de arquivos corrompidos ou incompletos.

## Estrutura do Projeto

```css
project/
│
├── src/
│   ├── __init__.py         # Inicialização do módulo
│   ├── functions.py        # Funções auxiliares do pipeline
│   ├── pipeline.py         # Classe principal contendo a lógica do pipeline
│
├── main.py                 # Ponto de entrada do programa
├── pyproject.toml          # Configuração do projeto
└── README.md               # Documentação do projeto
```


## Funcionalidades

1. **Download Inteligente**:
   - Realiza o download apenas de arquivos que não estão presentes na base.
   - Substitui automaticamente arquivos corrompidos por novas versões.

2. **Organização em Camadas**:
   - **Staging**: Recebe os arquivos zipados baixados do host.
   - **Raw**: Descompacta e organiza os dados brutos.
   - **Curated**: Realiza transformações e tratamento de dados.
   - **Sandbox**: Empilha e prepara os dados para análise.

3. **Empilhamento e Integração**:
   - Realiza a união de dados entre diferentes camadas.
   - Cria identificadores completos (e.g., `CNPJ_FULL`) para simplificar consultas.

4. **Suporte a Arquivos Corrompidos**:
   - Detecta e substitui automaticamente arquivos corrompidos.

5. **Exportação**:
   - Salva os resultados em formatos **Parquet** e **Delta**.

## Requisitos

- **Python**: >= 3.12
- **Spark**: Configurado e integrado ao Databricks.
- Dependências adicionais (listadas em `pyproject.toml`).

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/receita-pipeline.git
   cd receita-pipeline
    ```
2. Configure o ambiente virtual:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # No Windows: .venv\Scripts\activate
    ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Uso
1. Configure os diretórios no arquivo main.py:
    ```python
    staging = '/mnt/ADLS_Staging_datalakesenai/'
    raw = '/mnt/ADLS_raw_datalakesenai/'
    curated = '/mnt/ADLS_curated_datalakesenai/'
    sandbox = '/mnt/ADLS_Sandbox_datalakesenai/'
    path_rfb = 'Dados_Externos/Bases Cadastrais - Empresas/RFB - Base de CNPJs/'
    ```
2. Execute o pipeline:
    ```bash
    python main.py
    ```

## Fluxo de Processamento

1. Staging:

    Baixa os arquivos da Receita e remove versões antigas.

2. Raw:

    Extrai os dados zipados para pastas organizadas.

3. Curated:

    Trata e transforma os dados em formatos otimizados.
    Empilha os dados e realiza integrações finais.

## Contribuições

Contribuições são bem-vindas! Para reportar problemas ou sugerir melhorias, abra uma issue ou envie um pull request.

## Licença

Este projeto está licenciado sob a licença MIT. Consulte o arquivo LICENSE para mais detalhes.