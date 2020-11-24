import os
import boto3
import cv2
import json
from datetime import datetime, timedelta, timezone


def lambda_handler(event, context):
    # 処理後の画像をおくバケット名（再帰的呼び出しが怖いので、いったん別バケットに保存する）
    output_bucket_name = os.environ['output_bucket_name']

    s3 = boto3.resource('s3')

    # S3にアップされた画像の情報を取得する。
    input_bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    file_name = os.path.basename(object_key)

    # 画像をRekognitionで解析する。
    rekognition = boto3.client('rekognition')
    response = rekognition.detect_labels(Image={
        'S3Object': {
            'Bucket': input_bucket_name,
            'Name': object_key
        }
    })

    # 人数をカウントする
    cnt = 0

    # S3から画像をダウンロードする。
    tmp_dir = os.getenv('TMP_DIR', '/tmp/')
    bucket = s3.Bucket(input_bucket_name)
    bucket.download_file(object_key, tmp_dir + file_name)

    # 検出した人物に枠を描画する。
    image = cv2.imread(tmp_dir + file_name)
    height, width = image.shape[:2]

    for label in response['Labels']:
        if label['Name'] not in ['People', 'Person', 'Human']:
            continue
        for person in label['Instances']:
            cnt += 1
            box = person['BoundingBox']
            x = round(width * box['Left'])
            y = round(height * box['Top'])
            w = round(width * box['Width'])
            h = round(height * box['Height'])
            cv2.rectangle(image, (x, y), (x + w, y + h), (255, 255, 255), 3)

    cv2.imwrite(tmp_dir + file_name, image)

    # 描画後の画像をS3にアップロードする。
    outputBucket = s3.Bucket(output_bucket_name)
    outputBucket.upload_file(tmp_dir + file_name, file_name)
    os.remove(tmp_dir + file_name)

    # 人数を表示する
    print('検出された人数: {} 人'.format(cnt))
    
    # DynamoDBに人数を登録する
    dynamodb = boto3.resource('dynamodb')
    
    #指定テーブルのアクセスオブジェクト取得
    table = dynamodb.Table(os.environ['table_name'])
    
    #DynamoDBにレコード追加
    putResponse = table.put_item(
        Item={
        "MeasureDateTime": datetime.now(timezone(timedelta(hours=+9), 'JST')).strftime('%Y%m%d%H%M%S'),
        "value": cnt,
        "fileName": file_name
        }
    )