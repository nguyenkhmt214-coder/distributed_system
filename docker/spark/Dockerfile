FROM apache/spark:3.5.1

USER root

# 1. Cài unzip (để fix lỗi spark-submit giải nén venv bị fail)
# Thêm python3-dev để đảm bảo môi trường python chuẩn
RUN apt-get update && \
    apt-get install -y unzip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# 2. Nâng cấp PIP lên bản mới nhất (QUAN TRỌNG: pip cũ không tải được DuckDB wheel)
RUN python3 -m pip install --upgrade pip

# 3. Cài thư viện Python
# --only-binary=:all: -> BẮT BUỘC dùng file cài sẵn, CẤM compile (tránh tràn RAM)
# Chọn duckdb==0.10.3 hoặc 1.0.0 vì nó tương thích tốt nhất với Python 3.8 của Spark
RUN pip install --no-cache-dir \
    --only-binary=:all: \
    duckdb==1.0.0 \
    pandas \
    pyarrow

# 4. Quay về user spark
USER spark