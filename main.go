package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mmcdole/gofeed"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JsonFeeds []string

type Podcast struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Title       string             `bson:"title,omitempty"`
	Categories  []string           `bson:"categories,omitempty"`
	Link        string             `bson:"link,omitempty"`
	Description string             `bson:"description,omitempty"`
	Subtitle    string             `bson:"subtitle,omitempty"`
	Owner       PodcastOwner       `bson:"owner,omitempty"`
	Author      string             `bson:"author,omitempty"`
	Image       string             `bson:"image,omitempty"`
	Feed        string             `bson:"feed,omitempty"`
	PodlistUrl  string             `bson:"podlistUrl,omitempty"`
	Updated     time.Time          `bson:"updated,omitempty"`
}

type Episode struct {
	ID           primitive.ObjectID `bson:"_id,omitempty"`
	PodlistUrl   string             `bson:"podlistUrl,omitempty"`
	PodcastId    primitive.ObjectID `bson:"podcastId,omitempty"`
	PodcastUrl   string             `bson:"podcastUrl,omitempty"`
	PodcastTitle string             `bson:"podcastTitle,omitempty"`
	PodcastImage string             `bson:"podcastImage,omitempty"`
	Guid         string             `bson:"guid,omitempty"`
	Title        string             `bson:"title,omitempty"`
	Published    time.Time          `bson:"published,omitempty"`
	Duration     string             `bson:"Duration,omitempty"`
	Summary      string             `bson:"summary,omitempty"`
	Subtitle     string             `bson:"subtitle,omitempty"`
	Description  string             `bson:"description,omitempty"`
	Image        string             `bson:"image,omitempty"`
	Content      string             `bson:"content,omitempty"`
	Enclosure    EpisodeEnclosure   `bson:"enclosure,omitempty"`
}

type PodcastOwner struct {
	Name  string `bson:"name,omitempty"`
	Email string `bson:"email,omitempty"`
}

type EpisodeEnclosure struct {
	Filesize string `bson:"filesize,omitempty"`
	Filetype string `bson:"filetype,omitempty"`
	Url      string `bson:"url,omitempty"`
}

const (
	mongoURI          = "mongodb://localhost" // Consider moving this to an environment variable
	dbName            = "podgo"
	podcastCollection = "podcasts"
	episodeCollection = "episodes"
	maxConcurrent     = 10 // Limit concurrent operations
)

func LoadFeed(ctx context.Context, url string) (*gofeed.Feed, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURLWithContext(url, ctx)
	if err != nil {
		return nil, fmt.Errorf("feed error: %v", err)
	}
	if len(feed.FeedLink) <= 0 {
		feed.FeedLink = url
	}
	log.Printf("Feed Loaded: %s\n", url)
	return feed, nil
}

func GetTitleUrl(title string, otherPodcasts map[string]bool) string {
	t := TitleUrl(title)
	for otherPodcasts[t] {
		t += "x"
	}
	return t
}

func TitleUrl(title string) string {
	t := strings.ToLower(title)
	t = strings.NewReplacer("ä", "ae", "ö", "oe", "ü", "ue", "ß", "ss").Replace(t)
	re := regexp.MustCompile(`[^a-zA-Z0-9 ]`)
	t = re.ReplaceAllString(t, "")
	t = regexp.MustCompile(` +`).ReplaceAllString(t, "-")
	t = regexp.MustCompile(`-{2,10}`).ReplaceAllString(t, "-")
	return url.PathEscape(t)
}

func processFeed(ctx context.Context, feed *gofeed.Feed, podcastsCollection, episodesCollection *mongo.Collection, existingPodcastFeeds map[string]bool, podcastTitles map[string]bool) error {
	pTitleUrl := GetTitleUrl(feed.Title, podcastTitles)

	var podcast Podcast
	if existingPodcastFeeds[feed.FeedLink] {
		log.Printf("Updating existing podcast... %s\n", pTitleUrl)
		err := podcastsCollection.FindOne(ctx, bson.M{"feed": feed.FeedLink}).Decode(&podcast)
		if err != nil {
			return fmt.Errorf("error fetching existing podcast: %v", err)
		}
		// Update podcast info if needed
		updatePodcast(ctx, &podcast, feed, podcastsCollection)
	} else {
		log.Printf("Creating new podcast... %s\n", pTitleUrl)
		podcast = createNewPodcast(feed, pTitleUrl)
		_, err := podcastsCollection.InsertOne(ctx, podcast)
		if err != nil {
			return fmt.Errorf("error inserting podcast: %v", err)
		}
		existingPodcastFeeds[feed.FeedLink] = true
		podcastTitles[pTitleUrl] = true
	}

	// Process episodes
	err := processEpisodes(ctx, feed, podcast, episodesCollection)
	if err != nil {
		return fmt.Errorf("error processing episodes: %v", err)
	}

	return nil
}

func createNewPodcast(feed *gofeed.Feed, pTitleUrl string) Podcast {
	t := time.Now()
	if feed.PublishedParsed != nil {
		t = *feed.PublishedParsed
	}

	var o PodcastOwner
	var subtitle, author, image string
	if feed.ITunesExt != nil {
		if feed.ITunesExt.Owner != nil {
			o = PodcastOwner{Name: feed.ITunesExt.Owner.Name, Email: feed.ITunesExt.Owner.Email}
		}
		subtitle = feed.ITunesExt.Subtitle
		author = feed.ITunesExt.Author
		image = feed.ITunesExt.Image
	}

	return Podcast{
		Title:       feed.Title,
		Categories:  feed.Categories,
		Link:        feed.Link,
		Description: feed.Description,
		Subtitle:    subtitle,
		Owner:       o,
		Author:      author,
		Image:       image,
		Feed:        feed.FeedLink,
		PodlistUrl:  pTitleUrl,
		Updated:     t,
	}
}

func updatePodcast(ctx context.Context, podcast *Podcast, feed *gofeed.Feed, podcastsCollection *mongo.Collection) {
	// Update fields that might have changed
	update := bson.M{
		"$set": bson.M{
			"categories":  feed.Categories,
			"link":        feed.Link,
			"description": feed.Description,
			"updated":     time.Now(),
		},
	}

	if feed.ITunesExt != nil {
		update["$set"].(bson.M)["subtitle"] = feed.ITunesExt.Subtitle
		update["$set"].(bson.M)["author"] = feed.ITunesExt.Author
		update["$set"].(bson.M)["image"] = feed.ITunesExt.Image
	}

	_, err := podcastsCollection.UpdateOne(ctx, bson.M{"_id": podcast.ID}, update)
	if err != nil {
		log.Printf("Error updating podcast %s: %v\n", podcast.Title, err)
	}
}

func processEpisodes(ctx context.Context, feed *gofeed.Feed, podcast Podcast, episodesCollection *mongo.Collection) error {
	existingEpisodes := make(map[string]bool)
	cursor, err := episodesCollection.Find(ctx, bson.M{"podcastUrl": podcast.PodlistUrl})
	if err != nil {
		return fmt.Errorf("error fetching existing episodes: %v", err)
	}
	var episodes []Episode
	if err := cursor.All(ctx, &episodes); err != nil {
		return fmt.Errorf("error decoding existing episodes: %v", err)
	}
	for _, e := range episodes {
		existingEpisodes[e.Guid] = true
	}

	var newEpisodes []interface{}
	for _, e := range feed.Items {
		if e.ITunesExt != nil {
			if !existingEpisodes[e.GUID] {
				episode := createEpisode(e, podcast)
				newEpisodes = append(newEpisodes, episode)
			}
		}
	}

	if len(newEpisodes) > 0 {
		var operations []mongo.WriteModel
		for _, episode := range newEpisodes {
			operations = append(operations, mongo.NewInsertOneModel().SetDocument(episode))
		}

		_, err = episodesCollection.BulkWrite(ctx, operations)
		if err != nil {
			return fmt.Errorf("error inserting new episodes: %v", err)
		}
		log.Printf("Inserted %d new episodes for podcast %s\n", len(newEpisodes), podcast.Title)
	} else {
		log.Printf("No new episodes for podcast %s\n", podcast.Title)
	}

	return nil
}

func createEpisode(e *gofeed.Item, podcast Podcast) Episode {
	et := time.Now()
	if e.PublishedParsed != nil {
		et = *e.PublishedParsed
	}
	var ee EpisodeEnclosure
	if e.Enclosures != nil && len(e.Enclosures) > 0 {
		ee = EpisodeEnclosure{
			Filetype: e.Enclosures[0].Type,
			Filesize: e.Enclosures[0].Length,
			Url:      e.Enclosures[0].URL,
		}
	}

	var duration, summary, subtitle, image string
	if e.ITunesExt != nil {
		duration = e.ITunesExt.Duration
		summary = e.ITunesExt.Summary
		subtitle = e.ITunesExt.Subtitle
		image = e.ITunesExt.Image
	}

	return Episode{
		PodlistUrl:   GetTitleUrl(e.Title, make(map[string]bool)),
		PodcastUrl:   podcast.PodlistUrl,
		PodcastTitle: podcast.Title,
		PodcastImage: podcast.Image,
		Guid:         e.GUID,
		Title:        e.Title,
		Published:    et,
		Duration:     duration,
		Summary:      summary,
		Subtitle:     subtitle,
		Description:  e.Description,
		Image:        image,
		Content:      e.Content,
		Enclosure:    ee,
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	client := connectToMongoDB(ctx)
	defer client.Disconnect(ctx)

	database := client.Database(dbName)
	podcastsCollection := database.Collection(podcastCollection)
	episodesCollection := database.Collection(episodeCollection)

	createIndexes(ctx, podcastsCollection, episodesCollection)

	feeds := loadFeedsFromJSON("bak/feedbak.json")
	log.Printf("%d Podcast Feeds loaded from JSON File!\n", len(feeds))

	existingPodcastFeeds, podcastTitles := loadExistingPodcasts(ctx, podcastsCollection)

	processFeedsInBatches(ctx, feeds, podcastsCollection, episodesCollection, existingPodcastFeeds, podcastTitles)

	log.Println("All feeds processed!")
}

func connectToMongoDB(ctx context.Context) *mongo.Client {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to create MongoDB client: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB server: %v", err)
	}

	log.Println("Successfully connected to MongoDB")
	return client
}

func createIndexes(ctx context.Context, podcastsCollection, episodesCollection *mongo.Collection) {
	_, err := podcastsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "podlistUrl", Value: 1}},
	})
	if err != nil {
		log.Printf("Error creating index on podcasts collection: %v\n", err)
	}

	_, err = episodesCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "podcastUrl", Value: 1}},
	})
	if err != nil {
		log.Printf("Error creating index on episodes collection: %v\n", err)
	}
}

func loadFeedsFromJSON(filename string) []string {
	jsonFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var feeds []string
	if err := json.Unmarshal(byteValue, &feeds); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	return feeds
}

func loadExistingPodcasts(ctx context.Context, podcastsCollection *mongo.Collection) (map[string]bool, map[string]bool) {
	existingPodcastFeeds := make(map[string]bool)
	podcastTitles := make(map[string]bool)

	cursor, err := podcastsCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatalf("Failed to fetch existing podcasts: %v", err)
	}

	var podcasts []Podcast
	if err := cursor.All(ctx, &podcasts); err != nil {
		log.Fatalf("Failed to decode existing podcasts: %v", err)
	}

	for _, p := range podcasts {
		existingPodcastFeeds[p.Feed] = true
		podcastTitles[p.PodlistUrl] = true
	}

	return existingPodcastFeeds, podcastTitles
}

func processFeedsInBatches(ctx context.Context, feeds []string, podcastsCollection, episodesCollection *mongo.Collection, existingPodcastFeeds, podcastTitles map[string]bool) {
	batchSize := 10 // Process 10 feeds at a time
	for i := 0; i < len(feeds); i += batchSize {
		end := i + batchSize
		if end > len(feeds) {
			end = len(feeds)
		}

		processBatch(ctx, feeds[i:end], podcastsCollection, episodesCollection, existingPodcastFeeds, podcastTitles)

		log.Printf("Processed batch %d to %d\n", i, end-1)
		time.Sleep(5 * time.Second) // Sleep between batches to allow system to recover
	}
}

func processBatch(ctx context.Context, feeds []string, podcastsCollection, episodesCollection *mongo.Collection, existingPodcastFeeds, podcastTitles map[string]bool) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 3) // Reduce max concurrent operations

	for _, feedURL := range feeds {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			processFeedURL(ctx, url, podcastsCollection, episodesCollection, existingPodcastFeeds, podcastTitles)
		}(feedURL)
	}

	wg.Wait()
}

func processFeedURL(ctx context.Context, url string, podcastsCollection, episodesCollection *mongo.Collection, existingPodcastFeeds, podcastTitles map[string]bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	feed, err := LoadFeed(ctx, url)
	if err != nil {
		log.Printf("Error loading feed %s: %v\n", url, err)
		return
	}

	if err := processFeed(ctx, feed, podcastsCollection, episodesCollection, existingPodcastFeeds, podcastTitles); err != nil {
		log.Printf("Error processing feed %s: %v\n", url, err)
	}

	runtime.GC() // Force garbage collection after processing each feed
}
