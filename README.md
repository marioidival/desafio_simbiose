# Desafio Simbiose

Sincronização entre ElasticSearch e Cassandra

# Tabela de Conteúdo
- [Iniciando](#iniciando)
- [Executando](#executando)
- [Dependencias](#dependencias)
- [Arquivos](#arquivos)

## Iniciando

Criar um virtualenv (virtualenv ou virtualenvwrapper) e instalar as dependencias desse desafio:

	pip install -e requirements.txt

## Executando

O arquivo `simb_runner.py` fica responsável por iniciar o script de sincronização. Por padrão, o script ira verificar mudanças a cada 10 segundos.

	python simb_runner.py start

ou se quiser mudar o tempo de verificação:

	python simb_runner.py start --interval n # n = intervalo de verificações em segundos

## Dependencias

Os seguintes pacotes foram usados como dependências:
* [APScheduler](apscheduler.readthedocs.org) - Agendador de tarefas escrita em Python
* [click](click.pocoo.org/2/) - Interface de linha de comando escrita em Python
* [cassandra-driver](datastax.github.io/python-driver/getting_started.html) - Modulo Python do Cassandra
* [cqlengine](cqlengine.readthedocs.org) - Object Mapper do Cassandra
* [elasticsearch](elasticsearch-py.rtfd.org/) - Modulo Python do ElasticSearch
* [elasticsearch-dsl](elasticsearch-dsl.readthedocs.org) - Object Mapper do ElasticSearch
* [blist](stutzbachenterprises.com/blist/) - Dependencia direta do cassandra-driver

## Arquivos
	.
	|-- README.md
	|-- requirements.txt
	|-- simb_daemon.py
	|-- simb_datamodel
	|   |-- __init__.py
	|   |-- config_datamodel.py
	|   |-- simb_cassandra.py
	|   `-- simb_elasticsearch.py
	`-- simb_runner.py

	1 directory, 8 files

* `simb_daemon.py`: Contém manipuladores do Cassandra e Elasticsearch e classe que executa a tarefa de sincronização.
* `simb_runner.py`: Contém interface da linha de comando
* `simb_datamodel/config_datamodel.py`: Cria conexão com Cassandra e Elasticsearch, criando tabelas e indices.
* `simb_datamodel/simb_cassandra.py`: Contém data model do Cassandra e dicionario com os tipos CQL, mapeados com os tipos Python
* `simb_datamodel/simb_elasticsearch.py`: Contém data model com Elasticsearch
