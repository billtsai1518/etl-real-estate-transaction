#!/bin/bash

# init airflow connections


airflow connections add --conn-type http --conn-host https://datacenter.taichung.gov.tw/swagger/OpenData/117b24e0-4a33-4bab-9160-b9e6c1e92dcb real_estate_api

airflow connections add --conn-type postgres --conn-host postgres --conn-login airflow --conn-password airflow --conn-port 5432 postgres
