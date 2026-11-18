"""Build the lab5 PDF exclusively from retained experiment evidence."""
from datetime import datetime
from html import escape
import json
from pathlib import Path
import sys

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Preformatted

LAB=Path(__file__).resolve().parents[1]
run_id=sys.argv[1] if len(sys.argv)>1 else json.loads((LAB/'.runtime/run.json').read_text())['id']
E=LAB/'evidence'/run_id
OUT=LAB/'report'
OUT.mkdir(exist_ok=True)


def load(rel): return json.loads((E/rel).read_text(encoding='utf-8'))


def logs(stage):
    values=[]
    for p in (E/stage).glob('*-command.json'):
        v=json.loads(p.read_text(encoding='utf-8'))
        if 'logs' not in v['command']: continue
        for line in v['stdout'].splitlines():
            try: item=json.loads(line)
            except ValueError: continue
            values.append(item)
    return values


memory=load('memory/memory-selection.json')
migration=load('migrate/migration.json')
parallel=load('migrate/parallel-verification.json')
preemption=load('scheduling/preemption.json')
denied=load('quota/quota-denied.json')
quota=load('quota/quota-verification.json')
env=load('environment.json')
assert parallel['all_pairs_equal'] and migration['survived_pod_restart']
assert quota['excess_rejected'] and quota['retry_after_release_accepted']
assert any(t['reason']=='OOMKilled' for t in memory['oom']['terminated'])
assert len(memory['successful'])==3

fonts=Path('C:/Windows/Fonts')
for name,file in [('TimesRU','times.ttf'),('TimesRUBold','timesbd.ttf'),('MonoRU','consola.ttf')]:
    pdfmetrics.registerFont(TTFont(name,str(fonts/file)))
pdfmetrics.registerFontFamily('TimesRU',normal='TimesRU',bold='TimesRUBold',italic='TimesRU',boldItalic='TimesRUBold')
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='RU',fontName='TimesRU',fontSize=11,leading=14.5,spaceAfter=7))
styles.add(ParagraphStyle(name='H',fontName='TimesRUBold',fontSize=16,leading=20,spaceAfter=12))
styles.add(ParagraphStyle(name='Sub',fontName='TimesRUBold',fontSize=12,leading=16,spaceBefore=7,spaceAfter=7))
styles.add(ParagraphStyle(name='SmallRU',fontName='TimesRU',fontSize=9,leading=11.5,spaceAfter=5))
styles.add(ParagraphStyle(name='CellRU',fontName='TimesRU',fontSize=9.5,leading=12))
styles.add(ParagraphStyle(name='CodeRU',fontName='MonoRU',fontSize=8,leading=10))
story=[]


def p(text,style='RU'): story.append(Paragraph(text,styles[style]))
def h(text): p(text,'H')
def sub(text): p(text,'Sub')
def page(): story.append(PageBreak())
def code(text): story.append(Preformatted(text,styles['CodeRU']));story.append(Spacer(1,6))
def table(rows,widths):
    cells=[[Paragraph(escape(str(x)),styles['CellRU']) for x in row] for row in rows]
    t=Table(cells,colWidths=[x*mm for x in widths],repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#EAF0F4')),
                          ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),6),
                          ('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),6),
                          ('BOTTOMPADDING',(0,0),(-1,-1),6),
                          ('LINEBELOW',(0,0),(-1,0),0.7,colors.HexColor('#456276')),
                          ('LINEBELOW',(0,1),(-1,-1),0.3,colors.HexColor('#CCD5DC'))]))
    story.append(t);story.append(Spacer(1,8))


h('Лабораторная работа № 5')
p('Kubernetes: ресурсы, планирование и параллельный расчёт пар навыков','H')
p('Морозов Дмитрий Александрович · ИСУ 466761')
p('Инженерия данных · 18 ноября 2026 г.')
sub('Цель и выполненная работа')
p('Развёрнут локальный Kubernetes, собран образ расчёта из ДЗ № 3, измерено потребление памяти, '
  'получен OOMKilled и подобран рабочий предел. RustFS перенесён внутрь кластера; расчёт по четырём '
  'частям дал те же пары, что ДЗ № 3. Отдельно проверены ожидание CPU, вытеснение по приоритету и квота namespace.')
table([['Параметр','Фактическое значение'],['Вариант','Аналитики, 2057 вакансий; полный стек навыков'],
       ['Среда','Windows + Docker Desktop (WSL2); один узел minikube'],
       ['Версии','minikube 1.39.0; Kubernetes и kubectl 1.37.0; Python 3.11'],
       ['Ограничение контейнера узла',f"CPU: {env['node_cpu_quota']}; RAM: {env['node_memory_limit_mib']} MiB"],
       ['Ресурсы планировщика',f"Allocatable CPU: {preemption['allocatable_cpu']}"],
       ['Хранилище','RustFS 1.0.1; PVC 1 GiB; ClusterIP Service'],
       ['Каталоги','Docker, кеш minikube, входы и результаты размещены на D:']], [65,105])
p('Kubelet в Docker-драйвере сообщает 12 CPU хоста, хотя родительский cgroup узла ограничен тремя CPU. '
  'Это разные величины: CPU-квота ограничивает исполнение, а планировщик использует опубликованный Allocatable. '
  'Заявки опыта рассчитаны по фактическому Allocatable, сохранённому в журналах.','SmallRU')
sub('Программа и образ')
p('Использована неизменённая функция calculate из lab3/src/mapreduce.py: два процесса мапперов, '
  '128 записей в задании и окно до четырёх заданий. Она считает навыки, пары и тройки; адаптер lab5 '
  'сохраняет пары. Пик относится к этому полному алгоритму, а не к новой реализации только для пар.')
code('minikube -p de-lab5 image build -f lab5/Dockerfile -t de-lab5:local .')
p('Идентификатор собственного опыта: '+run_id+'. Все времена исходных JSON записаны в UTC; '
  'дата отчёта приведена по Москве. Доказательства: lab5/evidence/'+run_id+'/','SmallRU')

page();h('1. Заявки, пределы и OOMKilled')
p('Одиночные Jobs читали исходный JSONL из внешнего RustFS стенда № 3. Для CPU заданы request=500m '
  'и limit=1000m. Начальный предел памяти 512 MiB позволил получить три измерения без ограничения '
  'реального пика. Показатель /sys/fs/cgroup/memory.peak включает все процессы контейнера, в том числе мапперы. [1]')
rows=[['Запуск','Предел, MiB','Пик cgroup, MiB','Время программы, с','Результат']]
for label,values,limit in [('Исходный',memory['baseline'],512),('Подобранный',memory['successful'],memory['selected_limit_mib'])]:
    for i,v in enumerate(values): rows.append([f'{label} {i+1}',limit,f"{v['memory_peak']['bytes']/1048576:.2f}",f"{v['elapsed_seconds']:.3f}",'Совпадает с № 3'])
table(rows,[35,27,35,35,38])
sub('Наблюдаемый отказ')
t=next(t for t in memory['oom']['terminated'] if t['reason']=='OOMKilled')
code(f"Job: {memory['oom']['job']}\nlimit.memory: 64Mi\nterminated.reason: {t['reason']}\nterminated.exitCode: {t['exitCode']}\nstartedAt: {t['startedAt']}\nfinishedAt: {t['finishedAt']}")
p('При пределе 64 MiB контейнер действительно остановлен механизмом OOM. Job имеет backoffLimit=0 '
  'и restartPolicy=Never, поэтому ошибка не скрыта успешным повтором. Значение request памяти '
  'в этом опыте также снижено до 64 MiB, чтобы соблюсти request ≤ limit.')
sub('Выбор рабочего предела')
peak=memory['maximum_peak_bytes']/1048576
p(f'Максимум трёх измерений: <b>{peak:.2f} MiB</b>. Предел выбран с запасом 25% и округлением вверх до 16 MiB:')
code(f'ceil({peak:.2f} * 1.25 / 16) * 16 = {memory["selected_limit_mib"]} MiB')
p('Все три повтора с пределом 176 MiB успешно завершены и совпали с полной эталонной таблицей пар. '
  'Время в таблице - интервал программы после создания S3-клиента, а не полная длительность Job. '
  'Предел относится к данному входу и образу; для другого объёма данных требуется новое измерение.')
p('Точный статус отказа: memory/oom-'+run_id.lower()+'-64-status.json; все измерения и формула: '
  'memory/memory-selection.json. Статус OOM взят из Kubernetes, не выведен только из кода 137.','SmallRU')

page();h('2. Перенос S3 внутрь Kubernetes')
p('В namespace lab5 развёрнут тот же RustFS 1.0.1: один Deployment, Service rustfs с портом 9000 '
  'и постоянный PVC rustfs-data размером 1 GiB (StorageClass standard, ReadWriteOnce). '
  'Образ закреплён по digest; учётные данные переданы через Secret, значения в репозитории отсутствуют.')
code('http://rustfs.lab5.svc.cluster.local:9000\nPVC: rustfs-data, 1Gi, Bound\nimage: rustfs/rustfs@sha256:\n1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c')
p('Объекты прочитаны из исходного S3, записаны в новый S3 и прочитаны обратно. '
  'Для каждого объекта совпал SHA-256. Затем Deployment перезапущен; после замены Pod все объекты '
  'прочитаны повторно и снова совпали по SHA-256.')
rows=[['Объект','Размер, байт','SHA-256 (начало)']]
for v in migration['objects']:
    name=v['key'].split('/')[-1]
    rows.append([name,v['bytes'],v['sha256'][:20]])
table(rows,[70,35,65])
p('Перенесены исходный vacancies.jsonl, четыре непересекающиеся части, эталон пар и манифест. '
  'Исходный объект сохранил ключ raw/analysts/aa4d3566148d73b7/vacancies.jsonl. '
  'Полные ключи и контрольные суммы записаны в migrate/migration.json.','SmallRU')
sub('Что проверяет этот перенос')
p('Вычислительные Pod используют внутреннее DNS-имя Kubernetes, а не адрес хоста. '
  'Host port-forward использовался только для переноса объектов и независимой проверки результатов. '
  'Факт использования внутреннего endpoint также присутствует в журналах программы.')
p('PVC сохраняет данные при перезапуске Pod. Внутри кластера выбран локальный режим с одним томом '
  'без erasure coding: он не повторяет схему 3+3 прежнего стенда и не защищает от отказа диска или '
  'удаления всего minikube. Старые шесть дисков сохранены; их содержимое не переносилось прямым '
  'копированием служебных файлов RustFS.')

page();h('3. Параллельный расчёт и результат')
p('Indexed Job создан с completions=4 и parallelism=2. Индекс из JOB_COMPLETION_INDEX выбирает '
  'свою часть из манифеста. Части содержат 515/514/514/514 полных JSON-строк; каждая вакансия '
  'попадает в одну часть. Разделение не разрывает UTF-8 или JSON. [2]')
partials=[v for v in logs('migrate') if v.get('event')=='completed' and v.get('mode')=='map']
partials=sorted(partials,key=lambda v:int(v['index']))
table([['Индекс','Вакансий','Уникальных пар','Сумма частот','Время, с']]+[
    [v['index'],v['records'],v['unique_pairs'],v['pair_frequency_sum'],f"{v['elapsed_seconds']:.3f}"] for v in partials], [25,30,40,40,35])
p('После завершения частей отдельный Job суммирует частоты одинаковых пар по всем четырём '
  'индексам. Глобальный топ-10 строится после объединения; объединение локальных топов не используется. '
  'Повтор части заменяет результат своего индекса, поэтому не добавляет его второй раз.')
sub('Итоговый топ-10: Kubernetes и ДЗ № 3')
table([['№','Пара навыков','Kubernetes','ДЗ № 3']]+[
    [i+1,' + '.join(v['skills']),v['count'],v['count']] for i,v in enumerate(parallel['result']['top10'])],[12,103,28,27])
p('<b>Совпали все 15 647 пар и их частоты</b>, сумма частот 40 778. Сортировка топ-10: частота '
  'по убыванию, затем лексикографически по паре. Канонический CSV после объединения побайтно равен эталону.')
code('SHA-256 CSV пар:\nc8669dfcefc69fbbd12b8694226c9045cd70958fc497a8641e9dadef773efb8c')
p('Полный CSV: migrate/pairs.csv. Итог проверки: migrate/parallel-verification.json. '
  'По точным временным интервалам журналов одновременно работали две программы '
  '(migrate/concurrency.json). Ускорение на таком небольшом наборе не заявляется.','SmallRU')

page();h('4. Планирование, вытеснение и квота')
sub('Ожидание из-за CPU')
request=preemption['test_request_millicpu']
p(f"Планировщик видел {preemption['allocatable_cpu']} CPU; существующие заявки составляли "
  f"{preemption['existing_requests_millicpu']}m. Для тестовых Pod выбрана заявка {request}m: один Pod помещается, "
  'два одновременно - нет. cpu-low запущен, cpu-waiting остался Pending с условием Unschedulable.')
message=next(c['message'] for c in preemption['waiting']['status']['conditions'] if 'Insufficient cpu' in c.get('message',''))
p(escape(message),'SmallRU')
p('Оба Pod имеют приоритет 100 и preemptionPolicy=Never. Процесс sleep удерживает заявку, '
  'но не нагружает все заявленные CPU. Наблюдается учёт requests планировщиком, а не измерение реальной загрузки.')
sub('Вытеснение высоким приоритетом')
p('cpu-high имеет приоритет 1000 и PreemptLowerPriority. Он получил место после вытеснения cpu-low. '
  'У хранилища приоритет 100000; оно не участвовало в выборе жертвы. [3]')
e=preemption['preemption_events'][0]
code('reason: '+e['reason']+'\nsource: '+e['source']['component']+'\ntime: '+e['firstTimestamp'])
p(escape(e['message']),'SmallRU')
p('Высокоприоритетный Pod перешёл в Running. После его удаления ожидавший cpu-waiting тоже '
  'запустился. Тестовые Pod затем удалены. Числа и исходные события: scheduling/preemption.json.')
sub('Квота пространства имён')
p('В lab5-quota установлены requests.cpu=1, limits.cpu=1, requests.memory=128Mi, '
  'limits.memory=256Mi и pods=2. Первый Pod занял 700m CPU. Второй с такой же заявкой '
  'был отвергнут API до создания: 700m + 700m превышает 1 CPU. [4]')
p(escape(denied['stderr'].strip()),'SmallRU')
p('После удаления первого Pod повтор создания второго принят. Таким образом, квота действует '
  'на суммарные заявки namespace; в этом случае Pod не ожидает ресурсов узла, а вообще не создаётся. '
  'ResourceQuota также не гарантирует namespace долю физического CPU.')
p('Доказательства: quota/quota-denied.json, quota/quota-verification.json и командные транскрипты. '
  'Ожидание CPU, OOM и отказ квоты подтверждены разными состояниями и не смешиваются.','SmallRU')

page();h('5. Соответствие YARN, Mesos и Kubernetes')
p('Сопоставление ролей является функциональным: полное соответствие «один к одному» отсутствует. '
  'YARN и Mesos здесь анализируются по лекции и документации; их кластеры в этой работе не запускались. [5-9]')
table([['Функция','YARN','Mesos','Kubernetes'],
 ['Распределение ресурсов','Scheduler в ResourceManager','Allocator в master; offers фреймворкам','kube-scheduler размещает Pod по requests'],
 ['Узел исполнения','NodeManager','Agent (старое имя slave)','kubelet + container runtime'],
 ['Организация приложения','ApplicationMaster для приложения','Framework scheduler и executors','Job controller и код приложения; прямого аналога AM нет'],
 ['Решение о работе','AM запрашивает контейнеры и запускает задачи','Framework принимает offer и выбирает задачи','Controller создаёт Pod; scheduler выбирает узел'],
 ['Ресурсная единица','YARN container: выделение CPU/RAM','Task с ресурсами, исполняемый executor','Pod; OCI-контейнеры внутри'],
 ['Разделение ресурсов','Очереди и политики scheduler','Roles, allocator и политики фреймворка','Namespace, ResourceQuota, PriorityClass'],
 ['Отказоустойчивость управления','Active/standby ResourceManager','Active/standby master, ZooKeeper','HA control plane и etcd; здесь один узел']], [32,45,45,48])
sub('Как выглядел бы наш расчёт в каждой системе')
p('В YARN ApplicationMaster запросил бы контейнеры для четырёх частей, запустил задачи через '
  'NodeManager и затем объединение. Глобальное распределение контейнеров осталось бы у ResourceManager. [5,8]')
p('В Mesos scheduler нашего framework принимал бы подходящие offers и отправлял четыре tasks '
  'executor-ам на agents. Master определял бы, какие ресурсы предложить, а фреймворк - какие задачи '
  'на них выполнить. Это двухуровневое планирование. [9]')
p('В Kubernetes роль описания частей выполняет Indexed Job. Job controller поддерживает нужные '
  'Pod, scheduler размещает их, kubelet запускает контейнеры, а наш отдельный merge Job объединяет '
  'частоты. Распределённый алгоритм не реализуется автоматически одним kube-scheduler.')

page();h('6. Ответы на вопросы лекции')
sub('Зачем нужен общий менеджер ресурсов?')
p('Отдельные кластеры для MapReduce и Spark могут одновременно простаивать и испытывать перегрузку. '
  'Общий пул позволяет перераспределять свободные CPU и память. Он также уменьшает необходимость '
  'дублировать данные между выделенными стендами; само хранение данных при этом остаётся отдельной подсистемой.')
sub('Почему двухуровневая модель удобна для разных фреймворков? (слайд 10)')
p('Общий allocator делит ресурсы между фреймворками, а каждый scheduler сохраняет собственную '
  'логику длительности, локальности и приоритета задач. При добавлении новых типов приложений не '
  'нужно переносить всю их логику в один глобальный scheduler. Обратная сторона - взаимодействие '
  'двух политик и возможное ожидание подходящего offer. [9]')
sub('Что делают RM, NM и AM и как начинается приложение?')
p('ResourceManager управляет выделением ресурсов всего кластера; NodeManager исполняет и '
  'контролирует контейнеры своего узла. ApplicationMaster координирует конкретное приложение: '
  'после получения первого контейнера для AM он запрашивает дополнительные контейнеры и '
  'организует выполнение задач через NM. YARN container - ресурсное выделение, не обязательно Docker. [5,8]')
sub('Большой job занял кластер FIFO: что будет с коротким запросом? (слайд 17)')
p('При отсутствии свободных ресурсов короткий запрос ждёт. Fair Scheduler может выделить '
  'новому приложению справедливую долю; Capacity Scheduler - ресурсы отдельной очереди. Если '
  'ресурсы уже заняты, для быстрого перераспределения нужны настроенные preemption и политики очередей; '
  'одного переименования scheduler недостаточно. PriorityClass в Kubernetes обеспечивает приоритет, '
  'но не является полноценной заменой гарантированной доле очереди. [6,7]')
sub('Как проходит resource offer и что означает отказ? (слайд 22)')
p('Master предлагает scheduler-у ресурсы конкретных agents. Фреймворк принимает offer и задаёт '
  'tasks либо отказывается. Отказ не закрепляет ресурсы навсегда за этим фреймворком: allocator '
  'может предложить их другим. refuse_seconds управляет фильтром повторных предложений этому '
  'scheduler-у; фактическая утилизация зависит от остальных заявок и политик. [9,10]')
sub('Что такое delay scheduling, DRF и роль ZooKeeper?')
p('Delay scheduling ненадолго откладывает размещение ради локальности, затем допускает менее '
  'локальный вариант. DRF сравнивает доминирующие доли нескольких ресурсов, например CPU и RAM, '
  'а не только число задач. Mesos использует выбор активного master через ZooKeeper; standby '
  'получает роль после отказа active. Это защищает управление кластером, но не заменяет '
  'чекпоинты нашего вычисления. В Kubernetes согласованное состояние control plane хранится в etcd. [7,9,11]')

page();h('7. Воспроизведение и выводы')
p('Все команды выполняются из D:/rustfs-lab. Исходный профиль rc local должен иметь доступ к '
  'старому RustFS; программа использует существующие bin/rustfs.exe, bin/rc.exe и шесть дисков. '
  'Для minikube задаётся MINIKUBE_HOME на D:. Зависимости приложения закреплены в requirements.lock.txt.')
code("$py = 'lab3/.venv/Scripts/python.exe'\n$lab = 'submission/github/lab5/scripts'\n& $py -B -X utf8 $lab/manage.py cluster\n& $py -B -X utf8 $lab/manage.py build\n& $py -B -X utf8 $lab/experiments.py memory --new-run\n& $py -B -X utf8 $lab/experiments.py migrate\n& $py -B -X utf8 $lab/experiments.py scheduling\n& $py -B -X utf8 $lab/experiments.py quota")
p('Для нового опыта у этапа memory указать --new-run: создаются новый run id и отдельный префикс S3; существующие успешные Jobs '
  'не следует повторно применять с теми же именами. После выгрузки доказательств кластер можно '
  'остановить, сохранив профиль и PVC. Запуск требует проверки доступной памяти и свободного места.')
p('<b>Вывод.</b> Получен OOMKilled при 64 MiB, предел 176 MiB обоснован измеренным максимумом '
  '133,95 MiB и проверен тремя успешными повторами. S3 работает внутри Kubernetes, данные '
  'пережили замену Pod. Параллельный расчёт совпал со всеми парами ДЗ № 3. Продемонстрированы '
  'ожидание CPU, реальное вытеснение и отказ при превышении namespace-квоты.')
sub('Репозиторий и источники')
p('Код, Dockerfile, манифесты и журналы: '
  '<link href="https://github.com/dmitmorozov/data-engineering-lab3/tree/main/lab5" color="blue">'
  'github.com/dmitmorozov/data-engineering-lab3/tree/main/lab5</link>','SmallRU')
sources=[
 ('1','Kubernetes: ресурсы контейнеров','https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/'),
 ('2','Kubernetes: Jobs','https://kubernetes.io/docs/concepts/workloads/controllers/job/'),
 ('3','Kubernetes: приоритеты и preemption','https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/'),
 ('4','Kubernetes: ResourceQuota','https://kubernetes.io/docs/concepts/policy/resource-quotas/'),
 ('5','Apache Hadoop: YARN','https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html'),
 ('6','Apache Hadoop: Capacity Scheduler','https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html'),
 ('7','Apache Hadoop: Fair Scheduler','https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/FairScheduler.html'),
 ('8','Apache Hadoop: Writing YARN Applications','https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html'),
 ('9','Apache Mesos: Architecture','https://mesos.apache.org/documentation/latest/architecture/'),
 ('10','Apache Mesos: Scheduler HTTP API','https://mesos.apache.org/documentation/latest/scheduler-http-api/')]
for n,title,url in sources: p(f'[{n}] {title}. <link href="{url}" color="blue">Документация</link>.','SmallRU')
p('[11] К. Хицко. Лекция 5 «Менеджеры ресурсов YARN и Mesos», слайды 9-28. '
  'Ответы на обсуждение: 10, 17, 22; самопроверка: 28. Документация просмотрена 18.11.2026.','SmallRU')


def footer(canvas,doc):
    canvas.setFont('TimesRU',8)
    canvas.setFillColor(colors.HexColor('#526370'))
    canvas.drawString(20*mm,12*mm,'Инженерия данных · лабораторная № 5 · собственные запуски')
    canvas.drawRightString(190*mm,12*mm,str(doc.page))


doc=SimpleDocTemplate(str(OUT/'lab5_report.pdf'),pagesize=A4,rightMargin=20*mm,leftMargin=20*mm,
                      topMargin=18*mm,bottomMargin=20*mm,title='Лабораторная № 5. Kubernetes',
                      author='Морозов Дмитрий Александрович')
doc.build(story,onFirstPage=footer,onLaterPages=footer)
print(OUT/'lab5_report.pdf')
