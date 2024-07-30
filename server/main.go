package main

import (
	"github.com/gulmudovv/go-kafka-postgress/server/brokers"
	"github.com/gulmudovv/go-kafka-postgress/server/handlers"
	"github.com/gulmudovv/go-kafka-postgress/server/models"

	"github.com/gofiber/fiber/v2"
)

func main() {

	models.ConnectDB()
	go brokers.Consumer()

	app := fiber.New()

	api := app.Group("/api")
	api.Post("/comment", handlers.CreateComment)
	api.Get("/comments", handlers.GetAllComments)
	api.Get("/stats", handlers.GetStats)

	app.Listen(":80")

}
