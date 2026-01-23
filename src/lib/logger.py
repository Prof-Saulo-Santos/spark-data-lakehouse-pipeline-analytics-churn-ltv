# -------------------------
# Imports de bibliotecas padrão
# -------------------------

# Módulo padrão de logging do Python
import logging

# JSON para serialização estruturada dos logs
import json

# Socket para obter informações da máquina (hostname)
import socket

# Datetime para registro preciso do horário do evento
from datetime import datetime


class StructuredLogger:
    """
    Logger estruturado que gera logs em formato JSON.

    Objetivos:
    - Facilitar ingestão por sistemas de observabilidade (ELK, OpenSearch, Datadog)
    - Padronizar logs em pipelines de dados e MLOps
    - Permitir inclusão de metadados contextuais (ex: table_name, rows, source)
    """

    def __init__(self, name="CI360"):
        """
        Inicializa o logger estruturado.

        Parâmetros:
        - name: nome lógico do logger (normalmente o nome do serviço ou pipeline)
        """
        
        # Cria ou recupera um logger com o nome especificado
        self.logger = logging.getLogger(name)
        
        # Define o nível mínimo de log (INFO)
        self.logger.setLevel(logging.INFO)
        
        # Handler para saída em console (stdout)
        handler = logging.StreamHandler()
        
        # Formatter simples: a mensagem já estará em JSON
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        
        # Evita adicionar múltiplos handlers ao mesmo logger
        # (comum em execuções repetidas ou testes)
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        
        # Captura o hostname da máquina para rastreabilidade
        self.hostname = socket.gethostname()

    def _format(self, level, message, **kwargs):
        """
        Formata a mensagem de log em um objeto JSON.

        Campos padrão:
        - timestamp: data/hora em UTC
        - level: nível do log (INFO, ERROR, WARNING)
        - host: hostname da máquina
        - message: mensagem principal
        - kwargs: metadados adicionais fornecidos na chamada
        """
        
        log_entry = {
            # Timestamp em UTC para padronização em ambientes distribuídos
            "timestamp": datetime.utcnow().isoformat(),
            
            # Nível do log
            "level": level,
            
            # Host onde o processo está rodando
            "host": self.hostname,
            
            # Mensagem principal
            "message": message,
            
            # Metadados adicionais (ex: tabela, linhas, caminho)
            **kwargs
        }
        
        # Converte o dicionário para string JSON
        return json.dumps(log_entry)

    def info(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível INFO.
        """
        self.logger.info(
            self._format("INFO", message, **kwargs)
        )

    def error(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível ERROR.
        """
        self.logger.error(
            self._format("ERROR", message, **kwargs)
        )
    
    def warning(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível WARNING.
        """
        self.logger.warning(
            self._format("WARNING", message, **kwargs)
        )