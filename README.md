# Инженерия данных

Морозов Дмитрий Александрович, ИСУ 466761.

| Работа | Код | Отчёт |
| --- | --- | --- |
| № 3: RustFS, MapReduce и DuckDB | [lab3](lab3/) | [PDF](report/report.pdf) |
| № 4: Apache ZooKeeper | [lab4](lab4/) | [PDF](lab4/report/lab4_report.pdf) |

Python 3.11+, Docker Desktop с Linux-контейнерами. Код № 4 использует функции
MapReduce из № 3. Исходные данные и временные файлы в репозиторий не входят.

Запуск № 4 на рабочем стенде:

```powershell
Set-Location D:\rustfs-lab
$py = 'D:\rustfs-lab\lab3\.venv\Scripts\python.exe'
& $py -m pip install -r submission/github/lab4/requirements.lock.txt
& $py submission/github/lab4/scripts/prepare_local.py --workspace D:/rustfs-lab
Set-Location D:\rustfs-lab\submission\github
docker compose -f lab4/compose.yaml up -d zoo1 zoo2 zoo3
docker compose -f lab4/compose.yaml --profile experiment build coord-a
& $py lab4/scripts/run_ensemble_checks.py
& $py lab4/scripts/run_experiment.py --mode unsafe
& $py lab4/scripts/run_experiment.py --mode fenced
```

На другом стенде восстановить JSONL по `lab3/manifests/dataset.json`, эталонный CSV —
по `results/s3_results.json` или пересчётом № 3. Файлы входа:
`lab4/.runtime/input/vacancies.jsonl` и `reference.csv`.
Новые запуски создают временные данные и автоматически выполняют полную сверку.

```powershell
& $py -B -m unittest discover -s lab4/tests -v
```

Для интеграционных тестов запущенного ансамбля задать `LAB4_TEST_ZK`:
`127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183`.
