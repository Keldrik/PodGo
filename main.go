package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mmcdole/gofeed"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
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

func LoadFeed(url string, c chan *gofeed.Feed) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fp := gofeed.NewParser()
	feed, err := fp.ParseURLWithContext(url, ctx)
	if err == nil {
		if len(feed.FeedLink) <= 0 {
			feed.FeedLink = url
		}
		fmt.Println("Feed Loaded: ", url)
		fmt.Println("")
		c <- feed
	} else {
		fmt.Println("Feed Error: ", url)
		fmt.Println(err)
		fmt.Println("")
		c <- nil
	}
}

func GetTitleUrl(title string, otherPodcasts []string, extra string) string {
	t := title + extra
	t = TitleUrl(t)
	if TitleExist(otherPodcasts, t) {
		t = GetTitleUrl(t, otherPodcasts, "x")
	}
	return t
}

func TitleUrl(title string) string {
	t := strings.ToLower(title)
	t = strings.ReplaceAll(t, "ä", "ae")
	t = strings.ReplaceAll(t, "ö", "oe")
	t = strings.ReplaceAll(t, "ü", "ue")
	t = strings.ReplaceAll(t, "ß", "ss")
	var re = regexp.MustCompile(`[^a-zA-Z0-9 ]`)
	t = re.ReplaceAllString(t, "")
	var re2 = regexp.MustCompile(` +`)
	t = re2.ReplaceAllString(t, "-")
	var re3 = regexp.MustCompile(`-{2,10}`)
	t = re3.ReplaceAllString(t, "-")
	return url.PathEscape(t)
}

func TitleExist(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 600*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)
	database := client.Database("podgo")
	podcastsCollection := database.Collection("podcasts")
	episodesCollection := database.Collection("episodes")

	jsonFile, fileErr := os.Open("bak/feedbak.json")
	if fileErr != nil {
		fmt.Println(fileErr)
		return
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var feeds JsonFeeds
	jsonErr := json.Unmarshal(byteValue, &feeds)
	if jsonErr != nil {
		fmt.Println(jsonErr)
		return
	}
	fmt.Println(len(feeds), " Podcast Feeds loaded from Json File!")
	fmt.Println("----------------------------------------")
	fmt.Println("")

	cf := make(chan *gofeed.Feed)
	for i := 0; i < len(feeds); i++ {
		go LoadFeed(feeds[i], cf)
	}

	var podcasts []interface{}
	var episodes []interface{}

	var podcastTitles []string

	for i := 0; i < len(feeds); i++ {
		f := <-cf
		if f != nil {
			t := time.Now()
			if f.PublishedParsed != nil {
				t = *f.PublishedParsed
			}
			var o PodcastOwner
			if f.ITunesExt.Owner != nil {
				o = PodcastOwner{Name: f.ITunesExt.Owner.Name, Email: f.ITunesExt.Owner.Email}
			}
			podcast := Podcast{
				Title:       f.Title,
				Categories:  f.Categories,
				Link:        f.Link,
				Description: f.Description,
				Subtitle:    f.ITunesExt.Subtitle,
				Owner:       o,
				Author:      f.ITunesExt.Author,
				Image:       f.ITunesExt.Image,
				Feed:        f.FeedLink,
				PodlistUrl:  GetTitleUrl(f.Title, podcastTitles, ""),
				Updated:     t,
			}
			podcasts = append(podcasts, podcast)
			podcastTitles = append(podcastTitles, podcast.PodlistUrl)
			var episodeTitles []string
			for _, e := range f.Items {
				if e.ITunesExt == nil {
					break
				}
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
				episode := Episode{
					PodlistUrl:   GetTitleUrl(e.Title, episodeTitles, ""),
					PodcastUrl:   podcast.PodlistUrl,
					PodcastTitle: f.Title,
					PodcastImage: f.ITunesExt.Image,
					Guid:         e.GUID,
					Title:        e.Title,
					Published:    et,
					Duration:     e.ITunesExt.Duration,
					Summary:      e.ITunesExt.Summary,
					Subtitle:     e.ITunesExt.Subtitle,
					Description:  e.Description,
					Image:        e.ITunesExt.Image,
					Content:      e.Content,
					Enclosure:    ee,
				}
				episodes = append(episodes, episode)
				episodeTitles = append(episodeTitles, episode.PodlistUrl)
			}
		}
	}
	fmt.Println("Writing to DB...")
	podcastsCollection.Drop(ctx)
	pInsertResult, _ := podcastsCollection.InsertMany(ctx, podcasts)
	fmt.Println("Finished writing ", len(pInsertResult.InsertedIDs), "Podcasts!")
	episodesCollection.Drop(ctx)
	eInsertResult, _ := episodesCollection.InsertMany(ctx, episodes)
	fmt.Println("Finished writing ", len(eInsertResult.InsertedIDs), "Episodes!")
}
