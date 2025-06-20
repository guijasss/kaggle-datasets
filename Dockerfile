FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Copia e instala dependências
COPY requirements.txt .
RUN pip install -r requirements.txt \
 && pip install kaggle  # instala a API do Kaggle

# Copia todo o código da aplicação
COPY . .

RUN mv datasets/vcdb/data /tmp \
 && mv datasets/vcdb/docs /tmp

# Cria diretório padrão para a credencial do Kaggle
RUN mkdir -p /root/.config/kaggle

# Copie o kaggle.json da sua máquina no momento do build
COPY datasets/vcdb/kaggle/kaggle.json /root/.config/kaggle/kaggle.json

# Garante permissões seguras para a chave
RUN chmod 600 /root/.config/kaggle/kaggle.json

# Define o PYTHONPATH
ENV PYTHONPATH=/app

# Comando padrão
CMD ["python", "datasets/vcdb/main.py"]
