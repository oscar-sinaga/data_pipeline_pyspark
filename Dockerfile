FROM bitnami/spark:3.4.3

# 1. Install pip, jupyterlab, bash sebagai root
USER root
RUN apt-get update && apt-get install -y curl bash && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py && \
    pip3 install jupyterlab ipykernel

# 2. Install uv binary (tanpa Rust)
RUN curl -L https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-unknown-linux-gnu.tar.gz \
  | tar xz && \
  mv uv-x86_64-unknown-linux-gnu/uv /usr/local/bin/ && \
  rm -rf uv-x86_64-unknown-linux-gnu


# 3. Buat user non-root 'spark' dengan uid 1001 (kalau belum ada)
RUN useradd -m -u 1001 spark

# 4. Buat direktori home, cache, jupyter, dan set permission
# Membuat direktori home, cache, jupyter, dan tmp untuk pengguna spark
# serta mengatur permission untuk direktori-direktori tersebut
RUN mkdir -p /home/spark/.local /home/spark/.ivy2 /home/spark/.jupyter /tmp/jupyter_runtime && \
    # Mengatur permission untuk direktori-direktori tersebut agar dimiliki oleh pengguna spark
    chown -R spark:spark /home/spark /tmp/jupyter_runtime

# 5. Copy konfigurasi Jupyter
# Mengatur konfigurasi Jupyter untuk pengguna spark
# dengan mengatur token, password, ip, port, dan lain-lain
RUN echo "c.NotebookApp.token = ''" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur password Jupyter menjadi kosong
    echo "c.NotebookApp.password = ''" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur ip Jupyter menjadi 0.0.0.0 agar dapat diakses dari luar
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur Jupyter untuk tidak membuka browser secara otomatis
    echo "c.NotebookApp.open_browser = False" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur port Jupyter menjadi 8888
    echo "c.NotebookApp.port = 8888" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur shell command untuk Jupyter
    echo "c.NotebookApp.terminado_settings = {'shell_command': ['/bin/bash']}" >> /home/spark/.jupyter/jupyter_notebook_config.py && \
    # Mengatur permission untuk file konfigurasi Jupyter
    chown spark:spark /home/spark/.jupyter/jupyter_notebook_config.py

# 6. Set berbagai PATH dan ENV di bashrc
# Mengatur berbagai environment variable agar user 'spark' bisa menggunakan Spark dan Jupyter secara optimal
RUN echo 'export PS1="\u@\h:\w$ "' >> /home/spark/.bashrc && \
    # Menambahkan direktori bin Spark ke dalam PATH
    echo 'export PATH="/opt/bitnami/spark/bin:$PATH"' >> /home/spark/.bashrc && \
    # Menambahkan direktori bin Python ke dalam PATH
    echo 'export PATH="/opt/bitnami/python/bin:$PATH"' >> /home/spark/.bashrc && \
    # Menetapkan direktori HOME untuk user spark
    echo 'export HOME="/home/spark"' >> /home/spark/.bashrc && \
    # Menetapkan shell default sebagai bash
    echo 'export SHELL="/bin/bash"' >> /home/spark/.bashrc && \
    # Mengatur lokasi runtime Jupyter agar bisa ditulis di dalam container
    echo 'export JUPYTER_RUNTIME_DIR="/tmp/jupyter_runtime"' >> /home/spark/.bashrc && \
    # Mengatur lokasi konfigurasi Spark
    echo 'export SPARK_CONF_DIR="/opt/bitnami/spark/conf"' >> /home/spark/.bashrc && \
    # Menambahkan direktori kode aplikasi ke PYTHONPATH
    echo 'export PYTHONPATH="/app/src:$PYTHONPATH"' >> /home/spark/.bashrc && \
    # Mengatur PySpark agar suppress log ke level ERROR menggunakan log4j2
    echo 'export PYSPARK_SUBMIT_ARGS="--conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:/opt/bitnami/spark/conf/log4j2.properties --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:/opt/bitnami/spark/conf/log4j2.properties pyspark-shell"' >> /home/spark/.bashrc



# 7. Copy file konfigurasi & script
# Menyalin file log4j.properties ke direktori Spark conf
# agar dapat digunakan oleh Spark
COPY configs/spark_configs/log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
# Menyalin file run-all.sh ke root directory
# agar dapat dijalankan oleh pengguna spark
COPY scripts/run-all.sh /run-all.sh
# Mengatur izin execute untuk file run-all.sh
RUN chmod +x /run-all.sh

# 8. Tambahkan PostgreSQL JDBC driver
COPY driver/postgresql-42.6.0.jar /opt/bitnami/spark/jars/

# 9. Copy requirements.txt ke dalam image
COPY requirements.txt /app/requirements.txt
WORKDIR /app

# 10. Install Python dependencies pakai uv
RUN uv pip install --system -r requirements.txt

# 11. Install ipykernel sebagai user spark
USER spark
ENV HOME=/home/spark
RUN python3 -m ipykernel install --user --name pyspark --display-name "PySpark"

# 12. Set environment dan working directory
ENV SHELL=/bin/bash
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
WORKDIR /app

# 13. Jalankan JupyterLab sebagai user spark
ENTRYPOINT ["scripts/run-all.sh"]
