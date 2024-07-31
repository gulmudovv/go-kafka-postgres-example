package handlers

import (
	"encoding/json"
	"strings"

	"log"

	"github.com/gulmudovv/go-kafka-postgress/server/models"

	"github.com/gulmudovv/go-kafka-postgress/server/brokers"

	"github.com/gofiber/fiber/v2"
)

func CreateComment(c *fiber.Ctx) error {

	newComment := new(models.Comment)

	if err := c.BodyParser(newComment); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	if len(strings.Trim(newComment.Content, " ")) <= 0 {
		return c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Empty string",
		})

	}
	result := models.Db.Create(&newComment)
	if result.Error != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": result.Error,
		})

	}

	cmtInBytes, err := json.Marshal(newComment)
	if err != nil {
		log.Printf("Failed convert body into bytes and send it to kafka: %v", err)
	}
	err = brokers.PushCommentToQueue(cmtInBytes)
	if err != nil {
		log.Printf("Failed push message to kafka server: %v", err)
	}

	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment added successfully",
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}

func GetAllComments(c *fiber.Ctx) error {
	var comments []models.Comment

	result := models.Db.Find(&comments)

	if result.Error != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": result.Error,
		})

	}
	return c.JSON(&fiber.Map{
		"success": true,
		"data":    comments,
	})

}

func GetStats(c *fiber.Ctx) error {
	var comments []models.Comment

	result := models.Db.Where("processed = ?", 1).Find(&comments)
	if result.Error != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": result.Error,
		})

	}

	return c.JSON(&fiber.Map{
		"success":            true,
		"processed comments": result.RowsAffected,
	})
}
