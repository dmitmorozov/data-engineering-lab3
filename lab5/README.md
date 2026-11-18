# Лабораторная № 5: Kubernetes и управление ресурсами

Выполнено 18.11.2026 на собственном варианте «аналитики». Все практические опыты
завершены; журналы и применённые манифесты сохранены.

- [Итоговый PDF, 8 страниц](report/lab5_report.pdf)
- [Итоговый топ-10 пар](results/top10.csv)
- [Проверка результата](results/verification.json)
- [Все доказательства запуска 20261118T145018Z](evidence/20261118T145018Z/)

| Критерий | Наблюдение | Доказательство |
| --- | --- | --- |
| Кластер и образ | minikube 1.39.0, Kubernetes/kubectl 1.37.0, образ собран | `evidence/cluster/`, `evidence/20261118T145018Z/environment.json` |
| Остановка по памяти | `OOMKilled`, exit code 137, предел 64 MiB | `memory/oom-20261118t145018z-64-status.json` |
| Выбор предела | максимум 133.95 MiB; запас 25%, округление до 16 MiB → 176 MiB; три успешных повтора | `memory/memory-selection.json` |
| S3 в Kubernetes | RustFS 1.0.1, Service и PVC; SHA-256 семи объектов совпал после переноса и замены Pod | `migrate/migration.json` |
| Параллельный расчёт | 4 индекса, parallelism=2; фактически одновременно исполнялись две программы | `migrate/concurrency.json` |
| Совпадение № 3 | все 15647 пар и частоты совпали; сумма частот 40778 | `migrate/parallel-verification.json`, `migrate/pairs.csv` |
| Ожидание CPU | Pending, Insufficient cpu | `scheduling/waiting.json` |
| Вытеснение | событие Preempted для cpu-low, cpu-high запущен | `scheduling/preemption.json` |
| Квота namespace | отказ API при превышении 1 CPU; после освобождения повтор принят | `quota/quota-denied.json`, `quota/quota-verification.json` |

Пути в трёх последних столбцах, начинающиеся с `memory/`, `migrate/`, `scheduling/`
или `quota/`, относятся к `evidence/20261118T145018Z/`.

## Программа и ресурсы

`src/worker.py` вызывает неизменённую функцию `calculate` из lab3: полный стек,
два процесса мапперов, 128 записей в задании, окно до четырёх заданий. Она считает
навыки, пары и тройки; адаптер сохраняет только пары. Поэтому измеренная память
относится к этому алгоритму, а не к новой реализации исключительно для пар.

Вход разбит по границам JSON-строк на 515/514/514/514 записей. SHA-256 каждой части
проверяется при чтении. При объединении читается каждый completion index ровно
один раз, проверяются вход, число записей и контрольная сумма результата. Повтор
индекса заменяет его результат. Топ-10 выбирается после суммирования всех пар;
сортировка — частота по убыванию, затем лексикографически по паре.

Пик памяти — `/sys/fs/cgroup/memory.peak`, включая дочерние процессы. Три начальных
измерения: 133.95, 132.59 и 132.89 MiB. Выбор предела:
`ceil(133.95 * 1.25 / 16) * 16 = 176 MiB`.
Исходные Jobs используют CPU request=500m, limit=1000m, memory request=128Mi.
В OOM-опыте request и limit памяти равны 64Mi. Автоповтор отключён.

Docker ограничивает контейнер узла 3 CPU и 2560 MiB RAM. При этом kubelet видит
12 CPU хоста и публикует Allocatable=12 CPU. Scheduling-опыт использует фактическое
Allocatable: уже занято заявками 950m, тестовая заявка — 10950m. Это опыт учёта
requests; тестовый sleep не расходует столько CPU физически. На другом узле
контроллер рассчитывает размер заявки заново. Приоритеты: low=100, high=1000,
хранилище=100000. Сначала сохраняется Pending, затем создаётся high.

RustFS работает на одном PVC 1 GiB без erasure coding. Это перенос S3-объектов
через API в тот же RustFS 1.0.1, а не перенос служебных файлов шести старых дисков.
PVC выдержал замену Pod; отказ физического диска или удаление minikube не проверялись.
Локальный storage provisioner не обеспечивает аппаратную отказоустойчивость.

## Воспроизведение

Команды выполняются на исходном стенде `D:/rustfs-lab`. Нужны Docker Desktop/WSL2,
Python 3.11+, minikube в `bin/minikube.exe`, прежние RustFS/rc, профиль `rc local`,
исходный JSONL и эталон lab3. Данные и секреты в Git не включаются. На другом стенде
вход и эталон восстанавливаются по манифестам lab3 и инструкциям основного README.

```powershell
Set-Location D:/rustfs-lab
$py = 'lab3/.venv/Scripts/python.exe'
$lab = 'submission/github/lab5/scripts'
& $py -B -X utf8 $lab/manage.py preflight
& $py -B -X utf8 $lab/manage.py cluster
& $py -B -X utf8 $lab/manage.py build
& $py -B -X utf8 $lab/experiments.py memory --new-run
& $py -B -X utf8 $lab/experiments.py migrate
& $py -B -X utf8 $lab/experiments.py scheduling
& $py -B -X utf8 $lab/experiments.py quota
& $py -B -X utf8 $lab/build_report.py
```

`--new-run` создаёт отдельный идентификатор и префикс результатов S3. Последующие
этапы продолжают этот идентификатор. Существующие исходные доказательства остаются
в прежней папке. Применённые JSON-манифесты Kubernetes записываются рядом с журналами;
YAML в `manifests/` — читаемые шаблоны. `scheduling.yaml` не следует применять целиком:
объекты создаются последовательно контроллером для наблюдения промежуточных состояний.

Для локальной проверки без Kubernetes:

```powershell
& $py -B -X utf8 $lab/manage.py validate-local
```

Проверяются полный расчёт, сумма частей, повтор части без двойного счёта и отказ
при повреждении промежуточного CSV. Результаты этой проверки явно обозначены как
локальные и хранятся отдельно от Kubernetes-опытов.

Кластер запускается с 3 CPU и 2560 MiB RAM; перед новым запуском требуется не менее
4.5 GiB свободной RAM и 14 GiB на D:. Резерв хосту — 2 GiB RAM и 10 GiB диска.
Запас проверяется и во время работы. Кеши WSL после скачивания образов могут
временно занимать дополнительную память. `--disk-size` с Docker-драйвером не
является общей квотой Docker Desktop.

Docker-диск уже находится в `D:/Docker/DockerDesktopWSL/`. `MINIKUBE_HOME`, входы,
промежуточные результаты и временные файлы отчёта — в `lab5/.runtime/` на D:.
Исходный S3 при необходимости запускается отдельным процессом; его PID записывается
в `.runtime/host-rustfs.json`. Учётные данные передаются Kubernetes через stdin,
а приложению — через Secret. Содержимое Secret не записывается в доказательства.

Для проверки/пересборки PDF используются ReportLab 5.1.0, PyMuPDF 1.26.7 и Windows-шрифты
Times New Roman/Consolas. PyMuPDF нужен только для визуальной проверки; в образ программы
он не входит. Исходник отчёта — `scripts/build_report.py`, данные берутся из журналов.

## Теория и источники

В PDF разобраны соответствия компонентов YARN/Mesos/Kubernetes и вопросы лекции
со слайдов 10, 17, 22 и 28. Учтены отличия YARN container от OCI-контейнера,
ApplicationMaster от Job controller, ResourceQuota от доли очереди, OOM от preemption.
YARN и Mesos практически не развёртывались.

- [Ресурсы Kubernetes](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Приоритеты и вытеснение](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
- [ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)
- [YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
- [Mesos](https://mesos.apache.org/documentation/latest/architecture/)
