FROM python:3.7
EXPOSE 8080
WORKDIR /
COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
CMD streamlit run sales_order_app.py --server.port 8080