# PUBG Cheater Detection and Analysis

배틀그라운드 핵사용 유저 분석 및 Time Series Classification을 통한 핵 사용 유저 판별  

![pubg_image](https://images.logicalincrements.com/gallery/1920/822/PUBG-wallpaper-1.jpeg) 

 이 프로젝트의 목적은 핵 유저와 일반 유저의 게임 플레이 데이터를 수집, 비교, 분석한 후 sktime의 multivariate time series classification을 사용하여 핵 사용 유저 판별 모델을 만드는 것입니다.

<br>

## Table of Content
1. [데이터 불러오기](#데이터-불러오기)
1. [데이터 클리닝](#데이터-클리닝)
1. [Feature Selection](#Feature-Selection)
1. [Feature Mapping](#Feature-Mapping)
1. [Feature Scaling](#Feature-Scaling)
1. [Modeling](#Modeling)

<br>

## 데이터 불러오기
핵 사용 유저 아이디 3222개는 [카카오배그 홈페이지](https://pubg.game.daum.net) 에서 가져왔으며,  
2020년 2월1일부터 29일까지 핵 사용으로 인해 정지당한 계정입니다.  
일반 유저 아이디 3533개는 [Kaggle](https://www.kaggle.com/leonardokarl/pubg-statisctic) 에서 가져왔습니다.  
이후 위 소스에서 가져온 아이디의 게임정보를 [OPGG](https://pubg.op.gg) 에서 스크레이핑 했습니다.  
스크레이핑에 대한 자세한 코드는 [여기](https://github.com/alpacapetter/pubg-cheater-behavior-analysis/blob/main/user_data_scraper.py)를 참고해 주세요.

데이터를 parquet 파일로 저장했기에, 데이터 위에 바로 Spark.SQL을 사용하여 Feature engineering의 일부분을 간소화 하였습니다.  
```
df_cheater = spark.sql(
                        "SELECT *\
                        FROM parquet.`./cheater_data.parquet`\
                        WHERE started_at < '2020-03-01'\
                            AND started_at >'2019-01-01'\
                            AND type = 'official'\
                            AND (mode = 'tpp' OR mode = 'fpp')"
                        )
```
SQL 쿼리는 다음과 같은 requirement 에 의해 만들어졌습니다:
- started_at 이 2020-03-01 이전 데이터만 사용하여 정지 당했던 유저 데이터중 1년 후 정지가 풀린 후의 게임을 배제합니다.
- type 이 official 인것만 사용하여 kill수가 배틀로얄과 다른 event인 type을 배제합니다.
- mode 중 fpp와 tpp만 사용하여 비정상 empty mode row 를 배제합니다.

<br>

## 데이터 클리닝
데이터 클리닝 프로세스를 확인하시려면 [주피터 노트북](https://nbviewer.jupyter.org/github/alpacapetter/pubg-cheater-behavior-classification/blob/main/pubg_cheater_behavior_analysis.ipynb)을 확인하세요

<br>

## Feature Selection

![kills](https://i.imgur.com/lvq0huK.png)  
채택한 변수중 하나인 킬 수 에 대한 그래프 입니다.  
왼쪽의 핵 사용 유저의 킬 수와 오른쪽 일반 유저의 킬 수의 차이가 확실히 보입니다.  

<br>

![revive](https://i.imgur.com/orxBxyU.png)  
팀원을 살린 횟수 입니다.  
힐 아이템 사용 횟수와 같이 연관성이 없는 변수이기에 제외했습니다.  
이와 같이 연관성이 떨어지는 변수는 제외하고 연관성이 높은 변수만 사용하였습니다.

<br>

다음과 같은 변수를 채택하였습니다.
|변수|설명|
|----|----|
|mode|tpp, fpp|
|queue_size|1인, 2인, 4인|
|boosts|부스트 아이템 사용 수|
|death_type|사망 원인|
|kills|킬 수|
|headshot_kills|헤드샷 킬 수|
|assists|어시스트 수|
|kill_steaks|킬 스트리크|
|longest_kill|최장거리 킬 거리|
|damage_dealt|데미지|
|knock_downs|다운시킨 수|
|rank_percentile|최종등수 / 총 인원|  

>닉네임당 하루중 damage_dealt가 가장 높은 게임 데이터 7일치를 사용하였습니다.
총 플레이 기간이 7일 이하인 계정은 제외하였습니다.  

<br>

## Feature Mapping
문자 variable을 숫자로 맵핑해줍니다
```
df['mode']= df['mode'].map({'tpp':1, 'fpp':0})
df['death_type']= df['death_type'].map({'byplayer':0, 'suicide':1,'byzone':2, 'logout':3, 'alive':4})
```
queue size의 variable에 연속성을 부여합니다.
```
df['queue_size']= df['queue_size'].map({1:0, 2:1, 4:2})  
```

<br>

## Feature Scaling
sktime은 [Time series forest](https://www.sciencedirect.com/science/article/abs/pii/S0020025513001473) 알고리즘을 사용하기 때문에 사실 scaling에 크게 영향을 받지 않습니다. 하지만 정제된 데이터를 이후 다른 학습 알고리즘에 대입할 수 있도록 미리 scaling 해놓았습니다.  

![before_scaling](https://i.imgur.com/y5vHkLi.png)
![after_scaling](https://i.imgur.com/NsqgCPX.png)  
위는 Scaling 이전 이후 그래프 입니다.

<br>

## Modeling
### 모델링에 사용한 변수의 예시 그래프 입니다:

그래프의 왼쪽이 가장 최근 게임이고 오른쪽이 7일 전 게임입니다.  
![damage](https://i.imgur.com/QnokXXf.png)  
![kills](https://i.imgur.com/CWNjh3o.png)  

유저가 하루에 입힌 최대의 킬수(위)와 데미지(아래)를 7일간 나타낸 그래프 입니다.  
가장 최근(x=1)에 갑자기 높은 킬수와 데미지를 입히는 플레이어의 수가 비정상적으로 많아지는 것을 볼 수 있습니다.  
이와같이 time series 그래프를 통해 핵 사용 유저의 이상징후 패턴을 뚜렷하게 확인했습니다.  
이 그래프를 통해 많은 핵 유저 플레이어가 핵을 사용한지 하루만에 계정정지 당함을 알 수 있습니다.  
위와같은 변수 11개의 데이터를 모델에 사용합니다.

### scikit-learn이 아닌 sktime 사용 이유:
scikit-learn의 classification은 각 변수를 독립된 개체로 인식합니다.  
본 프로젝트는 각 게임이 아닌 최근 7일동안의 사용자 게임 패턴을 전체적으로 분석하여 classify을 하는것이 목적이기에 sktime의 Multivariate time series classification을 사용했습니다.  

> sktime의 자세한 사항은 [sktime repo](https://github.com/alan-turing-institute/sktime)를 참고하시기 바랍니다.

sktime은 세가지 Time series classification 방법을 지원합니다.  
그중 Column Ensembling은 최소 10개의 time series point가 필요하기때문에 사용하지 못했고, Bespoke classification algorithms은 아직 개발중이기에 사용하지 않았습니다.  
이와같은 이유로 ***Time series concatenation*** 알고리즘을 사용하였습니다.  

```
# 스코어의 unbiased 평균값을 구하기 위해 KFold cross validation을 사용했습니다.

steps = [
    ("concatenate", ColumnConcatenator()),
    ("classify", TimeSeriesForestClassifier(n_estimators=100)),
]
clf = Pipeline(steps)
scoring = 'accuracy'
score = cross_val_score(clf, X, y, cv=k_fold, n_jobs=1, scoring=scoring)
print(score)
print(round(np.mean(score)*100, 2))

>>>[0.884375   0.859375   0.896875   0.86875    0.875      0.884375
 0.85579937 0.85893417 0.89655172 0.89968652]
87.8
```

최근 7일간의 유저행동 패턴에 따라 ***87.8%*** 의 정확도로 유저의 핵 사용 여부를 판단합니다.  

>주의사항!  
위 모델은 핵 사용 판별이 아닌 이상징후를 파악하는 용도로 만들어졌습니다.  
트레이닝 데이터가 핵 사용 후 차단된 유저의 7일간 데이터이기에 차단되지 않고 핵을 지속적으로 사용해온 유저일 경우 핵사용 판별이 어렵습니다.  
낮은 숙련도의 유저의 계정을 매우 높은 숙련도의 제 3인이 갑자기 사용할 경우, 핵 사용이 아니더라도 핵 사용으로 classify 될 가능성이 있습니다.  
또한, 7일 이상의 사용 기록이 없는 새로운 유저일 경우, 데이터 부족으로 위 모델을 사용할 수 없습니다.
