## 수정 후 코드
import json
import requests
import pandas as pd
import pyarrow
import schedule
from google.cloud import bigquery
from datetime import datetime, timezone

class rawg():
    ## 클라이언트 지정하기
    service_account_path = './utopian-surface-268707-895e27478e4e.json' # 서비스키 받은걸로 경로 설정하면 클라이언트 불러오기 가능
    client = bigquery.Client.from_service_account_json(service_account_path)

    ## dataframe 적재 함수
    def load_dataframe(self, dataset_name, table_name, dataframe, type):
        table_id = rawg.client.dataset(dataset_name).table(table_name)
        job_config = bigquery.LoadJobConfig()
        if type == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND # 함수 옵션 설정하기
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE 
        job_config.autodetect = True # json은 스키마가 필요없지만 csv는 필요
        job = rawg.client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        ) 
        return job.result()

    ## 페이지에서 아이디별 리스트 만드는 함수(탑 100위 지정)
    def round_data(self, result, data, list_name):
        for i in range(len(data['results'])):
            if data['results'][i]['name'] in list_name:
                result['id'].append(data['results'][i]['id'])
                result['slug'].append(data['results'][i]['slug'])
                result['name'].append(data['results'][i]['name'])
                result['released'].append(data['results'][i]['released'])
                result['tba'].append(data['results'][i]['tba'])
                result['rating'].append(data['results'][i]['rating'])
                result['rating_top'].append(data['results'][i]['rating_top'])
                result['ratings'].append(data['results'][i]['ratings'])
                result['ratings_count'].append(data['results'][i]['ratings_count'])
                result['added'].append(data['results'][i]['added'])
                result['added_by_status'].append(data['results'][i]['added_by_status'])
                result['metacritic'].append(data['results'][i]['metacritic'])
                result['playtime'].append(data['results'][i]['playtime'])
                result['suggestions_count'].append(data['results'][i]['suggestions_count'])
                result['updated'].append(data['results'][i]['updated'])
                result['platforms'].append(data['results'][i]['platforms'])
                result['parent_platforms'].append(data['results'][i]['parent_platforms'])
                result['genres'].append(data['results'][i]['genres'])
                result['stores'].append(data['results'][i]['stores'])
                result['tags'].append(data['results'][i]['tags'])
                result['esrb_rating'].append(data['results'][i]['esrb_rating'])
                result['timeutcnow'].append(datetime.now(timezone.utc))
                result['timenow'].append(datetime.now())
                
            else:
                continue
            
        return result
        
    def top_100_list(self):    
        query = """
        SELECT game_name as name
        FROM `utopian-surface-268707.B4PLAY_TEST.TARGET_GAME_LIST`
        """
        
        query_job = rawg.client.query(query) # query함수 사용해서 query문 실행 

        top_100 = query_job.to_dataframe()
        
        return top_100
        
    ## 페이지 지정하여 적재하는 함수
    def rawg_game_list(self, page_size):
        url = "http://api.rawg.io/api/games?key=7c22e772f60348a5aac05d7bc4b18a31"
        req = url + "&page=1" + "&page_size=" + str(page_size)
        
        print(req)
        
        dataset_name = "ELLIE_DATASET"
        
        table_name = "test_set" # 테이블 네임
        
        result = {
            'id' : [],
            'slug' : [],
            'name' : [],
            'released' : [],
            'tba' : [],
            'rating' : [],
            'rating_top' : [],
            'ratings' : [],
            'ratings_count' : [],
            'added' : [],
            'added_by_status' : [],
            'metacritic' : [],
            'playtime' : [],
            'suggestions_count' : [],
            'updated' : [],
            'platforms' : [],
            'parent_platforms' : [],
            'genres' : [],
            'stores' : [],
            'tags' : [],
            'esrb_rating' : [],
            'timeutcnow' : [],
            'timenow' : []
        }
        
        cnt = 1 # 페이지 수
        
        top_100 = self.top_100_list()
        
        while req != None: # while로 변경하기
            # 만건 이상이면 save_data 저장   
            if len(result['id']) == 0:
                df_save = pd.DataFrame(result)
                
                df_save.to_csv('100_save.csv')
                
                self.load_dataframe(dataset_name, 'save_data', df_save, 'over')
                
            try : 
                request = requests.get(req)
                
                data = request.json()
                            
                self.round_data(result, data, top_100)
                    
                print('{} 진행 완료, {} 페이지'.format(cnt*(len(data['results'])), cnt))
        
                req = data['next']
                
                cnt += 1
            
            except: # 오류 예외 처리 하기, 데이터 프레임 초기화는 안전하기는 함
                
                df_result = pd.DataFrame(result)
            
                df_result.to_csv('fail_data.csv')
                
                self.load_dataframe(dataset_name, 'fail_data', df_result, 'over')
            
                print('{}개 적재 실패, {} 페이지, 주소: {}'.format(cnt*(len(data['results'])), cnt, req))
        
        df_result = pd.DataFrame(result)
        
        self.load_dataframe(dataset_name, table_name, df_result, 'append')
        
        print('적재 완료')    
            

        return df_result

test = rawg()

schedule.every().day.at("0:00").do(test.rawg_game_list(40))
