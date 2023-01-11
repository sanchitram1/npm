import threading
import logging 

class npmThreader(threading.Thread):

    def __init__(self, data, name, logger: logging.Logger):
        threading.Thread.__init__(self, target=self.run)
        self.name = name
        self.data = data # This is a package list 
        self.conn, self.cur = self.connect()
        self.logger = logger
        self.errors = []
        self.no_write = []
        self.npm_speed = []
        self.sql_speed = []
    
    def connect(self):
        import database 
        return database.connect('creds')
    
    def avg_speed(self, speed_list):
        if speed_list:
            return sum(speed_list) / len(speed_list)
        return 0 
    
    def stats(self, written):
        from collections import Counter
        errors = Counter([item['error'] for item in self.errors])
        no_write = Counter([item['error'] for item in self.no_write])
        self.logger.info(f'Thread_{self.name} Wrote {written}')
        self.logger.info(f'Thread_{self.name} Errors {errors.most_common()}')
        self.logger.info(f'Thread_{self.name} No_Write {no_write.most_common()}')
        self.logger.info(f'Thread_{self.name} NPM_Speed {self.avg_speed(self.npm_speed):.3f}')
        self.logger.info(f'Thread_{self.name} SQL_Speed {self.avg_speed(self.sql_speed):.3f}')
    
    def dump_errors(self):
        import json
        try:
            if self.errors:
                with open(f'./errors/errors_thread_{self.name}.json', 'w') as f:
                    json.dumps(self.errors, f)
            if self.no_write:
                with open(f'./errors/no_writes_thread_{self.name}.txt', 'w') as f:
                    for item in self.no_write:
                        f.write(f'{item}\n')
        except Exception as e:
            self.logger.error(f'Error writing errors: {e}')

    def npm_api(self, name):
        import requests 
        from requests.adapters import HTTPAdapter, Retry
        RETRY_STRATEGY = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=0.1
        )
        # Start API
        endpoint = f'http://registry.npmjs.org/{name}'
        sesh = requests.Session()
        sesh.mount(endpoint, HTTPAdapter(max_retries=RETRY_STRATEGY))
        data = []
        try:
            res = sesh.get(endpoint, stream=True)
            res.raise_for_status()
            data = res.json()
            data = data.decode("utf-8").replace(u"\u0000", "").encode("utf-8")
        except requests.exceptions.HTTPError as errh:
            self.errors.append({'name': name, 'error': type(errh).__name__})
            #self.logger.warning(f"Thread_{self.name} Http_Error")
        except requests.exceptions.ConnectionError as errc:
            self.errors.append({'name': name, 'error': type(errc).__name__})
            self.logger.warning(f"Thread_{self.name} Error_Connecting")
        except requests.exceptions.Timeout as errt:
            self.errors.append({'name': name, 'error': type(errt).__name__})
            self.logger.warning(f"Thread_{self.name} Timeout_Error")
        except requests.exceptions.RequestException as err:
            self.errors.append({'name': name, 'error': type(err).__name__})
            self.logger.warning(f"Thread_{self.name} OOps::Something_Else")
        finally:
            ret = {
                'endpoint': endpoint,
                'status_code': res.status_code,
                'pkg_name': name,
                'pkg_manager': 'npm',
                'response': data
            }    
            return ret

    def run(self):
        try:
            import time 
            written = 0
            N = int(len(self.data) / 5)
            for i,pkg in enumerate(self.data):
                if i % N == 0 and i > 0:
                    self.stats(written)
                start = time.time_ns()
                data = self.npm_api(pkg)
                npm = time.time_ns()
                written += self.write(data)
                sql = time.time_ns()
                self.npm_speed.append((npm-start)/1000000000)
                self.sql_speed.append((sql-npm)/1000000000)
            self.stats(written)
            self.logger.info(f'Thread_{self.name} Finished')
            self.dump_errors()
            self.conn.close()
        except Exception as e:
            self.logger.error(f'Thread_{self.name} Failed_bc {e}')
        finally:
            self.conn.close()

    def write(self, records):
        import json
        try:
            sql = 'INSERT INTO analysis.tbl_pkgs_src (endpoint, status_code, pkg_name, pkg_manager, response) \
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING'
            self.cur.execute(sql, 
                (
                    records['endpoint'],records['status_code'],records['pkg_name'],records['pkg_manager'], 
                    json.dumps(records['response']),
                )
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.no_write.append({'name': records['pkg_name'], 'error': e})
            self.logger.warning(f'Thread_{self.name} {type(e).__name__}')
        return self.cur.rowcount