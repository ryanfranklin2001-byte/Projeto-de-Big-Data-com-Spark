# Projeto de Big Data com PySpark – Análise de Dados de Food Delivery

## 📊 Descrição do Projeto

Este projeto tem como objetivo demonstrar na prática o funcionamento de um **ecossistema de Big Data** utilizando **Python, Apache Spark (PySpark)** e **Visual Studio Code**.

A aplicação realiza o processamento e análise de um conjunto de dados de pedidos de um sistema de **Food Delivery**, permitindo demonstrar conceitos como:

* Processamento de grandes volumes de dados
* Processamento distribuído
* Análise de dados com Spark
* Uso de DataFrames no PySpark
* Integração entre Python e Spark

Este projeto foi desenvolvido como atividade prática da disciplina **Ecossistema de Big Data** do curso de **Ciência de Dados**.

---

# 🧠 Tecnologias Utilizadas

* Python 3
* Apache Spark (PySpark)
* Java JDK 17
* Visual Studio Code
* Pandas
* Ambiente Virtual Python (venv)

---

# 📂 Estrutura do Projeto

```
projeto_bigdata/
│
├── scripts/
│   └── processamento.py
│
├── dados/               ← (DEVE SER CRIADA PELO USUÁRIO)
│   └── dados_loja_fooddelivery.csv
│
├── venv/
│
├── requirements.txt
│
├── .gitignore
│
└── README.md
```

⚠️ **A pasta `dados` não está no repositório GitHub porque foi adicionada ao `.gitignore`.**

Isso foi feito para evitar que **arquivos grandes de dados sejam enviados ao repositório**.

---

# 📥 Preparação do Ambiente

## 1️⃣ Instalar o Python

Baixe o Python em:

https://www.python.org/downloads/

Durante a instalação marque a opção:

```
Add Python to PATH
```

---

## 2️⃣ Instalar o Java (JDK 17)

Baixe o Java em:

https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html

Após a instalação verifique no terminal:

```
java -version
```

Saída esperada:

```
java version "17"
```

---

# ⚙️ Configuração do Projeto

## 1️⃣ Clonar o repositório

```
git clone https://github.com/seu-repositorio/projeto_bigdata.git
```

Entrar na pasta:

```
cd projeto_bigdata
```

---

## 2️⃣ Criar ambiente virtual

No terminal execute:

```
python -m venv venv
```

---

## 3️⃣ Ativar o ambiente virtual

### Windows

```
venv\Scripts\activate
```

### Linux/Mac

```
source venv/bin/activate
```

---

## 4️⃣ Instalar as dependências

Se existir o arquivo `requirements.txt`:

```
pip install -r requirements.txt
```

Ou instalar manualmente:

```
pip install pyspark pandas
```

---

# 📁 Criar a pasta de dados (OBRIGATÓRIO)

Como a pasta `dados` foi ignorada no GitHub, ela precisa ser criada manualmente.

Crie dentro do projeto:

```
dados
```

Estrutura esperada:

```
projeto_bigdata
│
├── dados
│   └── dados_loja_fooddelivery.csv
```

Depois coloque dentro dela o arquivo:

```
dados_loja_fooddelivery.csv
```

---

# ▶️ Executar o Projeto

Após configurar o ambiente e adicionar o dataset execute:

```
python scripts/processamento.py
```

---

# 📊 Resultado Esperado

Ao executar o script o Spark será inicializado e o terminal exibirá mensagens semelhantes a:

```
Spark funcionando!
```

Além disso o sistema poderá:

* carregar o dataset
* processar os dados
* realizar análises
* mostrar resultados no terminal

---

# 📚 Objetivos Educacionais

Este projeto permite aos alunos compreender:

* o funcionamento do **Apache Spark**
* como configurar um **ambiente de Big Data**
* como trabalhar com **grandes volumes de dados**
* processamento distribuído
* uso de **DataFrames no PySpark**

---

# 🧪 Possíveis Extensões do Projeto

Os alunos podem expandir o projeto realizando:

* análise de vendas por cidade
* análise de produtos mais vendidos
* criação de dashboards
* integração com Power BI
* processamento em tempo real

---

# 👨‍🏫 Autor

Projeto desenvolvido para fins educacionais na disciplina:

**Ecossistema de Big Data**
Curso: **Ciência de Dados**

---

# 📜 Licença

Este projeto é destinado exclusivamente para **uso educacional e acadêmico**.
Exibindo README.txt…