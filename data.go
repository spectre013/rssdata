package rssData

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/labstack/echo"
	"log"
	"time"
)

const DATABASE string = "newsmap"
const FEEDCOLLECTION string = "feeds"
const ITEMCOLLECTION string = "items"

type Feed struct {
	Id                 bson.ObjectId `json:"id" bson:"_id,omitempty"`
	Name               string        `json:"name" db:"name"`
	Url                string        `json:"url" db:"url"`
	BaseUrl            string        `json:"baseUrl" db:"baseurl"`
	SourceImageUrl     string        `json:"sourceImageUrl" db:"sourceimageurl"`
	FeedItemUrl        string        `json:"feedItemUrl" db:"feeditemurl"`
	SourceNameInternal string        `json:"sourceNameInternal" db:"sourcenameinternal"`
	ImageContentId     string        `json:"imageContentId" db:"imagecontentid"`
	Active             bool          `json:"active" db:"active"`
	PkiFingerprint     string        `json:"pkiFingerprint" db:"pkifingerprint"`
}

type Item struct {
	Id            bson.ObjectId `json:"id" bson:"_id,omitempty"`
	Title         string        `json:"title" db:"title"`
	Description   string        `json:"description" db:"description"`
	Source        string        `json:"source" db:"source"`
	SourceUrl     string        `json:"sourceUrl" db:"sourceurl"`
	SourceType    string        `json:"soruceType" db:"sourcetype"`
	Author        string        `json:"author" db:"author"`
	DatePublished time.Time     `json:"datePublished" db:"datepublished"`
	ContentName   string        `json:"contentName" db:"contentname"`
	Thumbnail     string        `json:"thumbnail" db:"thumbnail"`
	Bytes         []byte        `json:"bytes" db:"bytes"`
	Metadata      []MetaData    `json:"metadata" db:"metadata"`
	Entities      []Entity      `json:"entities"`
	RssDataUrl    string        `json:"rssDataUrl"`
	Feedid        string        `json:"_" db:"feedid"`
}

type Entity struct {
	Label string `json:"label" db:"label"`
	Text  string `json:"text" db:"text"`
	Start int    `json:"start" db:"start"`
	Stop  int    `json:"stop" db:"stop"`
}

type MetaData struct {
	Name    string `json:"name" db:"name"`
	Content string `json:"value" db:"content"`
}

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc echo.HandlerFunc
}

type Routes []Route

type Feeds struct {
	Session *mgo.Session
}

type Items struct {
	Session *mgo.Session
}

// #######################################################
//    FEEDS CRUD METHODS
// #######################################################
func (f Feeds) Create(feed Feed) (Feed, error) {
	c := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	err := c.Insert(feed)
	if err != nil {
		log.Println(err)
		return Feed{}, err
	}
	return feed, nil
}

func (f Feeds) Bulk(feeds []Feed) ([]Feed, error) {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	var fi []interface{}

	for _, fd := range feeds {
		fi = append(fi, fd)
	}
	err := col.Insert(fi...)
	fds := make([]Feed, 0)
	col.Find(nil).All(&fds)
	if err != nil {
		return fds, err
	}
	return fds, err
}

func (f Feeds) BulkUpdate(feeds []Feed) ([]Feed, error) {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	bulk := col.Bulk()

	for _, feed := range feeds {
		bulk.Update(bson.M{"_id": feed.Id}, feed)
	}
	_, err := bulk.Run()
	if err != nil {
		return make([]Feed, 0), err
	}
	fds, _ := f.Find()
	return fds, err
}

func (f Feeds) Find() ([]Feed, error) {
	feeds := make([]Feed, 0)
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	err := col.Find(nil).All(&feeds)
	if err != nil {
		return make([]Feed, 0), err
	}
	return feeds, nil
}

func (f Feeds) FindById(id string) (Feed, error) {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	var feed Feed
	err := col.FindId(bson.ObjectIdHex(id)).One(&feed)
	if err != nil {
		return feed, err
	}
	return feed, nil
}

func (f Feeds) FindBy(field string, value interface{}) ([]Feed, error) {
	feeds := make([]Feed, 0)
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	query := fmt.Sprintf(`{"%s":"%s"}"`, field, value)
	err := col.Find(query).All(&feeds)
	if err != nil {
		return make([]Feed, 0), err
	}
	return feeds, nil
}

func (f Feeds) Update(feed Feed) (Feed, error) {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	err := col.UpdateId(feed.Id, feed)
	if err != nil {
		return Feed{}, err
	}
	return feed, nil
}

func (f Feeds) Delete(feed Feed) bool {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	err := col.Remove(feed.Id)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (f Feeds) BulkDelete(feeds []Feed) bool {
	col := f.Session.DB(DATABASE).C(FEEDCOLLECTION)
	_, err := col.RemoveAll(nil)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

// #######################################################
//    Items CRUD METHODS
// #######################################################
func (i Items) Create(item Item) (Item, error) {
	c := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	_, err := c.Upsert(item.Id, item)
	if err != nil {
		log.Println(err)
	}
	return item, nil
}

func (i Items) Find() ([]Item, error) {
	items := make([]Item, 0)
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	err := col.Find(nil).All(&items)
	if err != nil {
		return make([]Item, 0), err
	}
	return items, nil
}

func (i Items) FindById(id string) (Item, error) {
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	var item Item
	err := col.FindId(bson.ObjectIdHex(id)).One(&item)
	if err != nil {
		return item, err
	}
	return item, nil
}

func (i Items) FindBy(field string, value interface{}) ([]Item, error) {
	items := make([]Item, 0)
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	query := fmt.Sprintf(`{"%s":"%s"}"`, field, value)
	err := col.Find(query).All(&items)
	if err != nil {
		return make([]Item, 0), err
	}
	return items, nil
}

func (i Items) Update(item Item) (Item, error) {
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	err := col.UpdateId(item.Id, item)
	if err != nil {
		return Item{}, err
	}
	return item, nil
}

func (i Items) Delete(item Item) bool {
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	err := col.Remove(item.Id)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (i Items) BulkDelete(items []Items) bool {
	col := i.Session.DB(DATABASE).C(ITEMCOLLECTION)
	_, err := col.RemoveAll(nil)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}
