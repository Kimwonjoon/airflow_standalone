# Airflow
### Add movies-dynamic-json.py
에어플로우에서 json 형식의 파일을 받아 spark sql로 처리
# Data
![image](https://github.com/user-attachments/assets/bced0661-4163-4483-9e45-dbd2de0c7c7c)

# Result
```
# 회사별 영화 개수
+---------------------------+--------+
|                    company|movieCnt|
+---------------------------+--------+
|   {20140909, (주)인사이...|       1|
|         {20158188, 사십이}|       1|
|   {20157248, (주)하이하...|       1|
|   {20111081, (주)스마일...|       1|
|    {20154468, 영화사 우상}|       1|
|  {20101112, 유한회사 제...|       1|
|       {20100880, CCRC S...|       1|
|   {20130184, 스마일컨텐츠}|       1|
| {20141688, 두물머리픽쳐스}|       1|
|   {20040050, 상상필름(주)}|       1|
|{2013214, (주)제이콘컴퍼니}|       1|
| {20138959, 더컨텐츠콤(주)}|       3|
|     {20122495, (주)두타연}|       1|
|{20114864, 세컨드윈드 필름}|       1|
|  {20104247, 라이온스 게...|       1|
| {20123221, 골든타이드픽...|       2|
|  {20157950, 영화사 키노...|       1|
|   {20062557, (주)트리필름}|       2|
|  {20100414, 영화사 도로...|       2|
|   {20100411, (주)화인웍스}|       1|
+---------------------------+--------+
only showing top 20 rows

# 감독별 영화 개수
+-------------------+--------+
|           director|movieCnt|
+-------------------+--------+
|           {안상훈}|       2|
|           {노진수}|       2|
|    {아람 라파포트}|       1|
|  {쿠니사와 미노루}|       1|
|    {카타오카 슈지}|       5|
|      {오쿠 와타루}|       3|
|           {박용집}|       1|
|          {우베 볼}|       2|
|      {아담 윈가드}|       1|
|{알렉상드르 헤보얀}|       1|
|      {조엘 에저튼}|       1|
|           {강의석}|       1|
|      {일라이 로스}|       1|
|           {문정윤}|       1|
|        {킴 파란트}|       1|
|           {윤재구}|       2|
|           {김봉주}|       1|
|           {박대열}|       2|
|           {배성상}|       1|
|{브리지트 르페브르}|       1|
+-------------------+--------+
only showing top 20 rows
```
