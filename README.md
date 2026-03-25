# Trade Flow Brasil

## Visão Geral

Trade Flow Brasil é um projeto de **Engenharia de Dados end-to-end** que analisa os fluxos de importação e exportação do Brasil utilizando APIs públicas oficiais de dados de comércio exterior.

O projeto aplica princípios modernos de arquitetura de dados, incluindo **processamento em camadas (Raw, Trusted, Refined)**, validação de qualidade de dados e **data warehousing em nuvem utilizando Google Cloud Platform (GCP)**.

O conjunto de dados final permite análises estratégicas sobre a **balança comercial brasileira**, principais produtos exportados/importados e relações comerciais internacionais.

---

## Objetivos do Projeto

- Identificar os principais produtos exportados e importados pelo Brasil  
- Construir uma pipeline de dados escalável utilizando dados de APIs públicas  
- Aplicar arquitetura de dados em camadas
- Implementar validações de qualidade de dados  
- Disponibilizar datasets analíticos em um data warehouse em nuvem  
- Permitir geração de insights de negócio a partir de dados governamentais brutos  

---

## Arquitetura

O projeto segue um modelo de **arquitetura em camadas**.

### Camada Raw

- Dados extraídos diretamente da API do ComexStat  
- Armazenados sem transformação  
- Dados históricos preservados  
- Armazenamento em **Google Cloud Storage**

---

### Camada Trusted (Dados Padronizados)

- Padronização de nomes de colunas  
- Garantia de tipos de dados  
- Tratamento de valores nulos  
- Remoção de duplicidades  
- Normalização dos dados  

---

### Camada Refined (Pronta para Análise)

- Carregamento das tabelas tratadas no **BigQuery**  
- Criação de tabelas analíticas utilizando **SQL**

---

## Stack Tecnológica

- Python  
- Pandas  
- Google Cloud Storage  
- BigQuery  
- SQL  
- Scripts de validação de qualidade de dados  

---

## Fluxo da Pipeline de Dados

1. Extração de dados da API pública de comércio exterior  
2. Armazenamento dos dados brutos no **Cloud Storage (RAW)**  
3. Transformação e limpeza dos dados (**Trusted**)  
4. Aplicação de regras de qualidade de dados  
5. Carregamento das tabelas tratadas no **BigQuery (Refined)**  
6. Disponibilização para consultas analíticas e dashboards  

---

## Regras de Qualidade de Dados

Exemplos de validações implementadas:

- Verificação de valores negativos de comércio  
- Códigos NCM inválidos ou nulos  
- Informações de país ausentes  
- Detecção de registros duplicados  
- Validação de consistência temporal  

Todos os logs de validação podem ser armazenados para **monitoramento e governança de dados**.

---

## Casos de Uso Analíticos

- Top 10 produtos exportados por valor  
- Principais países importadores  
- Análise de crescimento ano contra ano (YoY)  
- Desempenho de setores ao longo do tempo  

---

## Melhorias Futuras

- Estratégia de carregamento incremental  
- Tabelas particionadas e clusterizadas no BigQuery  
- Orquestração automática com **Airflow**  
- Integração com ferramentas de visualização (**Looker Studio / Power BI**)  

---

## Por que este projeto?

Este projeto demonstra:

- Boas práticas de Engenharia de Dados  
- Implementação de arquitetura em nuvem  
- Governança e controle de qualidade de dados  
- Modelagem de dados  
- Ingestão de dados via API  
- Transformação de dados orientada a negócio  

---

## Autor

**João Gabriel**  
Data Engineer & Analytics Enthusiast  
