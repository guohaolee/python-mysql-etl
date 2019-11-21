FROM python:3.7

WORKDIR /app

COPY requirements.txt ./

EXPOSE 5000

ENTRYPOINT ["/app/entrypoint.sh"]