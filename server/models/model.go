package models

import (
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Comment struct {
	ID        uint   `json:"id"`
	Content   string `json:"content" gorm:"text;not null;default:null"`
	Processed uint   `json:"processed" gorm:"not null;default:0"`
}

var Db *gorm.DB

func ConnectDB() {
	dsn := "host=db user=root password=test dbname=test_db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	if err != nil {
		log.Fatal("Failed to connect to database. \n", err)

	}
	log.Println("connected to database")

	Db = db
	log.Println("running migrations")
	Db.AutoMigrate(&Comment{})
}
