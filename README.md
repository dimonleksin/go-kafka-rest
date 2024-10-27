# Info
Access with token. Token generic with current time, username, password and random numbers, hash sha256
Token TTL 2 minuts

# List apis

## GET

+ / - info from project(version, author, mail...) ?
+ /token - get access token (in headers usernam: [user] password: [pass]) (creds from kafka user) ???
+ /topics - list of avalible topics ?
+ /topics/{topic.name}/describe - describe topics ?
+ /topics/{topic.name}/acls - acls of topics ?
+ /topics/{instance-id} - read from topic

## PUT

+ /groups/{gropId} - create client id and assign with group (in data json {"topic": "topic.name", "counter": 3}) where counter is a number of messages for read from topic with one request. returning path to instance(for exanple /topics/cg34-6nyxWmLwlNVZ1wNmjhhs2nuq4H4LNc)
+ /topics/{topic.name}/-/ - write data to topic ?
+ /topics/{topic.name}/{number of partition}/ - write data to partition from topic ?
+ /topics/{topic.name}/-/instance/{clientId}/commit - commit offset for consumer (in data {"offset": offsetId}) ?


## DELETE
+ /groups/{groupId}/{clientId} - deleting consumers

# Schema
+ при создании консюмера создаем отдельный поток, передаем в аргументах канал, сохраняем в массив структуру с именем консюмера и указателем на канал
+ при запросе записей по консюмеру находим нужную структуру, передаем в канал номер партиции(если надо) и обратно получаем записи
+ при удалении консюмера закрываем канал(поток завершится сам)
+ через 2 минуты после создания консюмера закрываем канал

