# Copyright (c) 2022 CSCIE88 Marina Popova
'''
This is a very simple Python application that reads one file, parses each line per the specified schema
(event_fields), and counts the number of lines in the file - by incrementing it's local counter (event_count);
It also increments a shared counter maintained in Redis - by 1 for each line - so that after all instances
of this application are done processing their own files - we have a total count of lines available in Redis.

In this app - we chose to increment the shared Redis counter per each processed line - to see the running total count
in progress in Redis; One could choose to increment the shared counter only once, when all line are counted locally -
to decrease the number of calls to Redis. However, in this approach - the shared counter will not show the running total
'''
import argparse
from collections import namedtuple
import redis

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
Event = namedtuple('Event', event_fields)


def parse_arguments():
    prog = "counter_process_redis"
    desc = "application that reads a file, parses all lines, counts the lines and " \
           "stores/increments the counter maintained in Redis"

    parser = argparse.ArgumentParser(prog=prog, description=desc)
    # name of a simple String field in Redis - that will be use as a shared counter
    parser.add_argument('--redis_counter_name', '-rc', required=False, default="counter")
    parser.add_argument('--file_name', '-f', required=False, default="../logs/file-input1.csv",
                        help="a csv log file to process")
    parser.add_argument('--redis_url', '-ru', required=False, default="redis://localhost:6379",
                        help="Redis end point url; Eg: redis://localhost:6379")

    parsed_args = parser.parse_args()
    return parsed_args


def do_work(redis_url, redis_counter_name, file_name):
    redis_client = redis.Redis.from_url(redis_url)
    event_count = 0
    # set initial value of the redis counter to 0 - if the counter does not exits yet
    #   (was not set by some other thread or app)
    
    Datant = {}
    Datau = {}
    if not redis_client.exists(file_name + "nt"):
        redis_client.hmset(file_name + "nt", Datant)
    if not redis_client.exists(file_name + "u"):
        redis_client.hmset(file_name + "u", Datau)
    redis_client.setnx(redis_counter_name, 0)
    with open(file_name) as file_handle:
        events = map(parse_line, file_handle)
        for event in events:
            Datant = decode_redis(redis_client.hgetall(file_name + "nt"))
            Datau = decode_redis(redis_client.hgetall(file_name + "u"))
            timestamp = event[1]
            url = event[2]
            user = event[3]
            key = timestamp.split("T")[0]+":" + timestamp.split("T")[1].split(":")[0]
            if key in Datant.keys():
                Datant[key].append(url)
            else:
                Datant[key] = [url,]
            key = key+url.split("=")[1]
            if key in Datau.keys():
                Datau[key].append(user)
                Datau[key].append(event[0])
            if event_count % 1000 == 0:
                print(f"processing event #{event_count} ... ")
            event_count += 1
            # increment Redis counter by 1
            redis_client.incr(redis_counter_name)
        if file_name == "file-input4.csv":
            comp_dict_nt = {**dict(decode_redis(redis_client.hgetall("../logs/file-input1.csvnt")))**dict(decode_redis(redis_client.hgetall("../logs/file-input2.csvnt")))**dict(decode_redis(redis_client.hgetall("../logs/file-input3.csvnt")))**dict(decode_redis(redis_client.hgetall("../logs/file-input4.csvnt")))}
            for key, value in comp_dict_nt.items():
                if key in dict(decode_redis(redis_client.hgetall("../logs/file-input1.csvnt"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input2.csvnt"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input3.csvnt"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input4.csvnt"))):
                    comp_dict_nt[key] = [value , comp_dict_nt[key]]
            comp_dict_u = {**dict(decode_redis(redis_client.hgetall("../logs/file-input1.csvu")))**dict(decode_redis(redis_client.hgetall("../logs/file-input2.csvu")))**dict(decode_redis(redis_client.hgetall("../logs/file-input3.csvu")))**dict(decode_redis(redis_client.hgetall("../logs/file-input4.csvu")))}
            for key, value in comp_dict_u.items():
                if key in dict(decode_redis(redis_client.hgetall("../logs/file-input1.csvu"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input2.csvu"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input3.csvu"))) and key in dict(decode_redis(redis_client.hgetall("../logs/file-input4.csvu"))):
                    comp_dict_u[key] = [value , comp_dict_u[key]]
            query = input("Query? (1/2/3)>")
            if query == '1':
                filename = input("date? Should be in format year/month/day:Hour\n")
            urls = []
            for url in comp_dict_nt[filename]:
                if url in urls:
                    continue
                else:
                    urls.append(url)
            print(len(urls))

        elif query == '2':
            date = input("date? Should be in format year/month/day:Hour\n")
            url = input("Url?>")
            filename = date+":"+url.split("=")[1]  
            urls = []
            for url in comp_dict_u[filename]:
                if url in urls:
                    continue
                elif url.split('-')[0] == "user":
                    urls.append(url)
            print(len(urls))
        elif query == '3':
            date = input("date? Should be in format year/month/day:Hour\n")
            url = input("Url?>")
            filename = date+":"+url.split("=")[1]
    
        urls = []
        for url in comp_dict_u[filename]:
            if url in urls:
                continue
            elif not url.split('-')[0] == "user":
                urls.append(url)
        print(len(urls))
        
        shared_counter = redis_client.get(redis_counter_name)
        print(f"processing of {file_name} has finished processing with local event_count={event_count}, "
              f"shared counter from Redis: {shared_counter}")
        redis_client.hmset(file_name + "u", Datau)
        redis_client.hmset(file_name + "nt", Datant)
    redis_client.close()


def parse_line(line):
    return Event(*line.split(','))


def main():
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
    file_name = parsed_args.file_name
    redis_url = parsed_args.redis_url
    do_work(redis_url, redis_counter_name, file_name)


if __name__ == '__main__':
    main()

def decode_redis(src):
    if isinstance(src, list):
        rv = list()
        for key in src:
            rv.append(decode_redis(key))
        return rv
    elif isinstance(src, dict):
        rv = dict()
        for key in src:
            rv[key.decode()] = decode_redis(src[key])
        return rv
    elif isinstance(src, bytes):
        return src.decode()
    else:
        raise Exception("type not handled: " +type(src))
