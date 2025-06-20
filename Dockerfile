FROM python:3.11-slim

WORKDIR /app

# 1. Copia só o requirements.txt primeiro
COPY requirements.txt .

# 2. Instala as dependências antes de copiar o código (usa cache se o arquivo não mudou)
RUN pip install --no-cache-dir -r requirements.txt

# 3. Agora copia o resto do código (essa camada será refeita com frequência)
COPY . .

# 4. Define o PYTHONPATH
ENV PYTHONPATH=/app

# 5. Move diretórios se necessário (como falamos antes)
RUN mv datasets/vcdb/data /tmp/data \
 && mv datasets/vcdb/docs /tmp/docs

# 6. Comando final
CMD ["python", "datasets/vcdb/main.py"]
