// webservice
// image upload
// pg dependency
// rabbitmq

package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/middleware/logger"
	"github.com/kataras/iris/v12/middleware/recover"
	"github.com/lithammer/shortuuid/v3"
	"github.com/minio/minio-go"
	"github.com/streadway/amqp"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

func main() {
	fmt.Println("XD")
	fmt.Println("Image pull policy works")
	ctx := context.Background()

	// Database
	postgresUsername := os.Getenv("POSTGRES_USERNAME")
	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	postgresHost := os.Getenv("POSTGRES_HOST")

	// MinIO Object store
	minioEndpoint := os.Getenv("MINIO_HOST") + ":9000"
	minioAccessKeyID := "lGSjnwo7rqLjl8TYZ3s1"                         // TODO make env variable
	minioSecretAccessKey := "dXy1ZscEHL4So5FHFCYLmCTtTX3PK1K6506JF1wX" // TODO make env variable
	minioUseSSL := false
	bucketName := "infiles" // TODO make env variable

	rabbitmqEndpoint := os.Getenv("RABBITMQ_HOST")

	dsn := "postgres://" + postgresUsername + ":" + postgresPassword + "@" + postgresHost + ":5432/postgres?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())
	defer sqldb.Close()

	err := createSchema(ctx, db)
	failOnError(err, "Error while creating schema")

	// Initialize minio client object.
	minioClient, err := minio.New(minioEndpoint, minioAccessKeyID, minioSecretAccessKey, minioUseSSL)
	failOnError(err, "Error while creating minio client")
	fmt.Println("Connected to minio")
	err = createIfNotExistsMinioBucket(minioClient, bucketName)
	failOnError(err, "Error while creating minio bucket")

	// RabbitMQ
	conn, err := amqp.Dial("amqp://default_user_Kv2WwZT2lMCOn-xcW_c:KVaX1V7rfXKW4kwG7w0MpzcHXWZ8PpAT@" + rabbitmqEndpoint + ":5672/") // TODO Make username, password, host and port configurable using ENV variables.
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// RabbitMQ - Channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// RabbitMQ - Queue
	q, err := ch.QueueDeclare(
		"images", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Println("About to run app")
	runApplication(db, minioClient, bucketName, ch, q)
}

// Image represents an image.
type Image struct {
	Id              int64  `json:"id" bun:"id,pk,autoincrement"`
	Name            string `json:"name"`
	StorageLocation string `json:"storage_location"`
}

func (i *Image) String() string {
	return fmt.Sprintf("Image<%d %s %s>", i.Id, i.Name, i.StorageLocation)
}

func createSchema(ctx context.Context, db *bun.DB) error {
	_, err := db.NewCreateTable().
		Model((*Image)(nil)).
		IfNotExists().
		Exec(ctx)

	if err != nil {
		return err
	}

	return nil
}

func createIfNotExistsMinioBucket(minioClient *minio.Client, bucketName string) error {
	// MinIO Make a new bucket called "images".
	location := "us-east-1" // Leave this to "us-east-1"

	// Check if the bucket already exists (which happens if you run this twice)
	exists, errBucketExists := minioClient.BucketExists(bucketName)

	if errBucketExists == nil && exists {
		log.Printf("MinIO: The bucket %s already exists \n", bucketName)
		return nil
	}
	err := minioClient.MakeBucket(bucketName, location)

	if err == nil {
		log.Printf("MinIO: Successfully created the bucket %s\n", bucketName)
	}

	return err
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(err)
	}
}

func setGetHandler(app *iris.Application, db *bun.DB) {
	// Method: GET
	// Resource http://localhost:8080
	app.Handle("GET", "/", func(ctx iris.Context) {
		var images []Image
		exists, err := db.NewSelect().Model(&images).Order("id ASC").Exists(ctx)

		if !exists {
			fmt.Println("image does not exist")
		}

		if err != nil {
			panic(err)
		}

		if images == nil {
			images = []Image{}
		}

		ctx.JSON(images)
	})
}

func setPostHandler(app *iris.Application, db *bun.DB, minioClient *minio.Client, bucketName string, ch *amqp.Channel, q amqp.Queue) {
	app.Handle("POST", "/", func(ctx iris.Context) {
		var image Image

		// Receive the incoming file
		// See also: https://github.com/kataras/iris/blob/c4843a4d82aae53518bb7c247923007d1d99893c/_examples/file-server/upload-file/main.go
		file, info, err := ctx.FormFile("image")

		if err != nil {
			ctx.StatusCode(iris.StatusInternalServerError)
			ctx.HTML("Fileupload: Error while uploading: " + err.Error() + "\n" + info.Filename)
			return
		}

		defer file.Close()

		// Upload the zip file to MinIO
		fileName := shortuuid.New() + "_" + info.Filename

		objectSize := info.Size
		objectReader := bufio.NewReader(file)

		fmt.Printf("Fileupload: Receiving file with path: " + fileName + "\n")

		// Upload the zip file with FPutObject
		n, err := minioClient.PutObject(bucketName, fileName, objectReader, objectSize, minio.PutObjectOptions{})
		if err != nil {
			log.Fatalln(err)
		}

		log.Printf("MinIO: Successfully uploaded %s of size %d\n", fileName, n)

		image.Name = fileName
		image.StorageLocation = bucketName + "/" + fileName

		// Write to the DB
		_, err = db.NewInsert().Model(&image).Exec(ctx)
		if err != nil {
			ctx.Writef("PG database error: " + err.Error())
			return
		}

		body, err := json.Marshal(image)
		if (err) != nil {
			panic(err)
		}

		// Write to the message queue
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         []byte(body),
			},
		)

		ctx.Writef("Success: %s %s", image.Name, image.StorageLocation)
	})
}

func runApplication(db *bun.DB, minioClient *minio.Client, bucketName string, ch *amqp.Channel, q amqp.Queue) {
	// Web Service
	app := iris.New()
	app.Logger().SetLevel("debug")

	// Recover panics
	app.Use(recover.New())
	app.Use(logger.New())

	setGetHandler(app, db)
	setPostHandler(app, db, minioClient, bucketName, ch, q)

	app.Run(iris.Addr(":8080"), iris.WithoutServerError(iris.ErrServerClosed))
}
