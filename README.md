# serverless-postgre-demo## GlueとAurora(PostgreSQL)を繋ぐ手順

1. **Glue Databaseの作成**  
   - まずは枠だけ作成（中身は後でCrawlerで作成）

2. **Glue Connectionの作成**  
   - タイプ: JDBC  
   - 接続先: Aurora PostgreSQLのエンドポイント  
   - JDBC URL例: `jdbc:postgresql://<aurora-endpoint>:5432/<dbname>`  
   - ユーザー名・パスワード・VPC・サブネット・セキュリティグループを指定

3. **Glue-Aurora間の通信許可**  
   - Glueのセキュリティグループ（SG）のインバウンドに「AuroraのSG」をソースとした全ての通信を許可  
   - GlueのSGのアウトバウンドは `0.0.0.0/0` でOK  
   - AuroraのSGのインバウンドに「GlueのSG」からの通信を許可

4. **Glue-S3間の通信許可**  
   - S3エンドポイント（VPCエンドポイント）を作成  
   - Glueが利用するサブネットのルートテーブルにS3エンドポイントを追加

5. **Glue Crawlerの作成**  
   - 作成したConnectionを指定  
   - Glue Databaseを指定  
   - Crawler実行でスキーマを自動作成

6. **IAMロールの設定**  
   - GlueServiceRole（Glue用のマネージドポリシー）  
   - SecretsManager（Auroraの認証情報をSecretで管理する場合はアクセス権限も付与）

これで初めてETLを書く準備が整います。
パスワードのencryptionが違うらしい