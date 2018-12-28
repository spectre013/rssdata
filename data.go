package rssData

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo"
	"log"
	"strings"
	"time"
)

const DATABASE string = "gotopic"

//const FEEDCOLLECTION string = "feeds"
//const ITEMCOLLECTION string = "items"
//const MONGOURI string = "localhost"
const CACHE = "ingestcache"

type Feed struct {
	Id                 string `json:"id" db:"id"`
	Name               string `json:"name" db:"name"`
	Url                string `json:"url" db:"url"`
	BaseUrl            string `json:"baseUrl" db:"baseurl"`
	SourceImageUrl     string `json:"sourceImageUrl" db:"sourceimageurl"`
	FeedItemUrl        string `json:"feedItemUrl" db:"feeditemurl"`
	SourceNameInternal string `json:"sourceNameInternal" db:"sourcenameinternal"`
	ImageContentId     string `json:"imageContentId" db:"imagecontentid"`
	Active             bool   `json:"active" db:"active"`
	PkiFingerprint     string `json:"pkiFingerprint" db:"pkifingerprint"`
}

type Item struct {
	Id            string     `json:"id" db:"id"`
	Title         string     `json:"title" db:"title"`
	Description   string     `json:"description" db:"description"`
	Source        string     `json:"source" db:"source"`
	SourceUrl     string     `json:"sourceUrl" db:"sourceurl"`
	SourceType    string     `json:"soruceType" db:"sourcetype"`
	Author        string     `json:"author" db:"author"`
	DatePublished time.Time  `json:"datePublished" db:"datepublished"`
	ContentName   string     `json:"contentName" db:"contentname"`
	Thumbnail     string     `json:"thumbnail" db:"thumbnail"`
	Bytes         []byte     `json:"bytes" db:"bytes"`
	Metadata      []MetaData `json:"metadata" db:"metadata"`
	Entities      []Entity   `json:"entities"`
	RssDataUrl    string     `json:"rssDataUrl"`
	Feedid        string     `json:"_" db:"feedid"`
}

type Entity struct {
	Id     string `json:"id" db:"id"`
	Label  string `json:"label" db:"label"`
	Text   string `json:"text" db:"text"`
	Start  int    `json:"start" db:"start"`
	Stop   int    `json:"stop" db:"stop"`
	Itemid string `json:"_" db:"itemid"`
}

type MetaData struct {
	Id      string `json:"id" db:"id"`
	Name    string `json:"name" db:"name"`
	Content string `json:"value" db:"content"`
	Itemid  string `json:"_" db:"itemid"`
}

var feedColumns = []string{"id", "name", "url", "baseurl", "sourceimageurl", "feeditemurl", "sourcenameinternal", "imagecontentid", "active", "pkifingerprint"}
var itemColumns = []string{"id", "title", "description", "source", "sourceurl", "sourcetype", "author", "datepublished", "contentname", "thumbnail", "bytes", "rssdataurl", "feedid"}
var entityColumns = []string{"id", "label", "text", "start", "stop", "itemid"}
var metaColumns = []string{"id", "name", "content", "itemid"}

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc echo.HandlerFunc
}

type Routes []Route

type Data struct {
	DB    *sqlx.DB
	Debug bool
}

var FeedsTable = `CREATE TABLE IF NOT EXISTS public.feeds (
	id uuid NOT NULL,
	"name" text,
	url text NOT NULL,
	baseurl text,
	sourceimageurl text,
	feeditemurl text,
	sourcenameinternal text,
	imagecontentid text,
	active bool,
	pkifingerprint text,
	PRIMARY KEY(id)
);`

var ItemTable = `CREATE TABLE IF NOT EXISTS public.items (
	id uuid NOT NULL,
	title text,
	description text, 
	source text,
	sourceurl text,
	sourcetype text,
	author text,
	datepublished timestamptz, 
	contentname text, 
	thumbnail text,
	bytes bytea,
	rssdataurl text,
	feedid uuid NOT NULL,
	PRIMARY KEY(id)
);
CREATE INDEX IF NOT EXISTS items_feedid ON public.items (feedid);
CREATE INDEX IF NOT EXISTS items_sourceurl ON public.items (sourceurl);
`

var MedataDataTable = `CREATE TABLE IF NOT EXISTS public.metadata (
	id uuid NOT NULL,
	name text,
	content text,
	itemid uuid NOT NULL,
	PRIMARY KEY(id)
);
CREATE INDEX IF NOT EXISTS metadata_itemid ON public.metadata (itemid);`

var EntityTable = `CREATE TABLE IF NOT EXISTS public.entities (
	id uuid NOT NULL,
	label text,
	text text,
	start int,
	stop int,
	itemid uuid NOT NULL,
	PRIMARY KEY(id)
);
CREATE INDEX IF NOT EXISTS entities_itemid ON public.entities (itemid);
`

type Feeds struct {
	DB *sqlx.DB
}

type Items struct {
	DB *sqlx.DB
}

type Meta struct {
	DB *sqlx.DB
}

type Entities struct {
	DB *sqlx.DB
}

func CreateTable(schema string, db *sql.DB) bool {
	_, err := db.Exec(schema)
	if err != nil {
		log.Println("Error creating for table ,", err)
		return false
	}
	return true
}

func buildQuery(table string, querytype string, columns []string) string {
	query := ""
	switch querytype {
	case "insert":
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ","), ":"+strings.Join(columns, ", :"))
		break
	case "update":
		cols := make([]string, 0)
		query = fmt.Sprintf("UPDATE %s set ", table)
		for _, v := range columns {
			if v != "id" {
				cols = append(cols, fmt.Sprintf("%s=:%s ", v, v))
			}
		}
		query += strings.Join(cols, ", ") + " where id = :id"
		break
	case "delete":
		query = "delete from entities where id = :id"
		break
	default:
		query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ","), table)
	}

	//log.Println("Debug: ", query)
	return query
}

// #######################################################
//    FEEDS CRUD METHODS
// #######################################################
func (f Feeds) Create(feed Feed) (Feed, error) {
	_, err := f.DB.NamedExec(buildQuery("feeds", "insert", feedColumns), &feed)
	if err != nil {
		log.Println("Error Adding Feed ", feed.Id)
		log.Println(err)
		return Feed{}, err
	}
	return feed, nil
}

func (f Feeds) Bulk(feeds []Feed) ([]Feed, error) {
	tx := f.DB.MustBegin()
	for _, feed := range feeds {
		_, err := tx.NamedExec(buildQuery("feeds", "insert", feedColumns), &feed)
		if err != nil {
			log.Println("Error Adding Feed ", feed.Id)
			log.Println(err)
			tx.Rollback()
			return []Feed{}, err
		}
	}
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Println("Error committing feed to database rolling back transaction")
	}
	return feeds, nil
}

func (f Feeds) BulkUpdate(feeds []Feed) ([]Feed, error) {
	tx := f.DB.MustBegin()
	for _, feed := range feeds {
		_, err := tx.NamedExec(buildQuery("feeds", "update", feedColumns), &feed)
		if err != nil {
			log.Println("Error Updating Feed ", feed.Id)
			log.Println(err)
			tx.Rollback()
			return []Feed{}, err
		}
	}
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Println("Error committing feed to database rolling back transaction")
	}
	return feeds, nil
}

func (f Feeds) Find() ([]Feed, error) {
	feeds := make([]Feed, 0)
	err := f.DB.Select(&feeds, buildQuery("feeds", "", feedColumns)+" ORDER BY name ASC")
	if err != nil {
		log.Println("Error Finding Feeds")
		log.Println(err)
		return feeds, err
	}
	return feeds, nil
}

func (f Feeds) FindById(id string) (Feed, error) {
	feed := Feed{}
	err := f.DB.Get(&feed, buildQuery("feeds", "", feedColumns)+" WHERE id=$1", id)
	if err != nil {
		log.Println(err)
		return feed, err
	}
	return feed, nil
}

func (f Feeds) FindBy(field string, value interface{}) ([]Feed, error) {
	feeds := make([]Feed, 0)
	queryString := fmt.Sprintf(buildQuery("feeds", "", feedColumns)+" WHERE %s=$1", field)
	err := f.DB.Select(&feeds, queryString, value)
	if err != nil {
		log.Println(err)
		return feeds, err
	}
	return feeds, nil
}

func (f Feeds) Update(feed Feed) (Feed, error) {
	_, err := f.DB.NamedExec(buildQuery("feeds", "update", feedColumns), &feed)
	if err != nil {
		log.Println(err)
		return Feed{}, err
	}
	return feed, nil
}

func (f Feeds) Delete(feed Feed) bool {
	_, err := f.DB.NamedExec(buildQuery("feeds", "delete", feedColumns), &feed)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (f Feeds) BulkDelete(feeds []Feed) bool {
	tx := f.DB.MustBegin()
	for _, feed := range feeds {
		_, err := tx.NamedExec(buildQuery("feeds", "delete", feedColumns), &feed)
		if err != nil {
			log.Println(err)
			return false
			tx.Rollback()
		}
	}
	err := tx.Commit()
	if err != nil {
		log.Println(err)
		return false
		tx.Rollback()
	}
	return true
}

// #######################################################
//    Items CRUD METHODS
// #######################################################
func (i Items) Create(item Item) (Item, error) {
	_, err := i.DB.NamedExec(buildQuery("items", "insert", itemColumns), &item)
	if err != nil {
		log.Println("Error Adding Item ", item.Id)
		log.Println(err)
		return Item{}, err
	}
	return item, nil
}

func (i Items) Find() ([]Item, error) {
	items := make([]Item, 0)
	err := i.DB.Select(&items, buildQuery("items", "", itemColumns)+" ORDER BY name ASC")
	if err != nil {
		log.Println("Error Finding Feeds")
		log.Println(err)
		return items, err
	}

	return items, nil
}

func (i Items) FindById(id string) (Item, error) {
	item := Item{}
	err := i.DB.Get(&item, buildQuery("items", "", itemColumns)+" WHERE id=$1", id)
	if err != nil {
		log.Println(err)
		return item, err
	}
	return item, nil
}

func (i Items) FindBy(field string, value interface{}) ([]Item, error) {
	items := make([]Item, 0)
	queryString := fmt.Sprintf(buildQuery("items", "", itemColumns)+" WHERE %s=$1", field)
	err := i.DB.Select(&items, queryString, value)
	if err != nil {
		log.Println(err)
		return items, err
	}
	return items, nil
}

func (i Items) Update(item Item) (Item, error) {
	_, err := i.DB.NamedExec(buildQuery("items", "update", itemColumns), &item)
	if err != nil {
		log.Println(err)
		return Item{}, err
	}
	return item, nil
}

func (i Items) Delete(item Item) bool {
	_, err := i.DB.NamedExec(buildQuery("items", "delete", itemColumns), &item)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (i Items) BulkDelete(items []Items) bool {
	tx := i.DB.MustBegin()
	for _, item := range items {
		_, err := tx.NamedExec(buildQuery("items", "delete", itemColumns), &item)
		if err != nil {
			log.Println(err)
			return false
			tx.Rollback()
		}
	}
	err := tx.Commit()
	if err != nil {
		log.Println(err)
		return false
		tx.Rollback()
	}
	return true
}

// #######################################################
//    METADATA CRUD METHODS
// #######################################################
func (m Meta) Create(metadata MetaData) (MetaData, error) {
	_, err := m.DB.NamedExec(buildQuery("metadata", "insert", metaColumns), &metadata)
	if err != nil {
		log.Println("Error Adding Metadata ", metadata.Id)
		log.Println(err)
		return MetaData{}, err
	}
	return metadata, nil
}

func (m Meta) Bulk(metadatas []MetaData) ([]MetaData, error) {
	tx := m.DB.MustBegin()
	itemid := ""
	for _, metadata := range metadatas {
		itemid = metadata.Itemid
		_, err := tx.NamedExec(buildQuery("metadata", "insert", metaColumns), &metadata)
		if err != nil {
			log.Println("Error Adding record ", metadata.Id)
			log.Println(err)
			tx.Rollback()
			return []MetaData{}, err
		}
	}
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Println("Error committing metadata to database rolling back transaction for item id ", itemid)
	}
	return metadatas, nil
}

func (m Meta) Find() ([]MetaData, error) {
	metadata := make([]MetaData, 0)
	err := m.DB.Select(&metadata, buildQuery("metadata", "", metaColumns)+" ORDER BY name ASC")
	if err != nil {
		log.Println("Error Finding Feeds")
		log.Println(err)
		return metadata, err
	}
	return metadata, nil
}

func (m Meta) FindById(id string) (MetaData, error) {
	metadata := MetaData{}
	err := m.DB.Get(&metadata, buildQuery("metadata", "", metaColumns)+" WHERE id=$1", id)
	if err != nil {
		log.Println(err)
		return metadata, err
	}
	return metadata, nil
}

func (m Meta) FindBy(field string, value interface{}) ([]MetaData, error) {
	metadata := make([]MetaData, 0)
	queryString := fmt.Sprintf(buildQuery("metadata", "", metaColumns)+" WHERE %s=$1", field)
	err := m.DB.Get(&metadata, queryString, value)
	if err != nil {
		log.Println(err)
		return metadata, err
	}
	return metadata, nil
}

func (m Meta) Update(metadata MetaData) (MetaData, error) {
	_, err := m.DB.NamedExec(buildQuery("metadata", "update", metaColumns), &metadata)
	if err != nil {
		log.Println(err)
		return MetaData{}, err
	}
	return metadata, nil
}

func (m Meta) Delete(metadata MetaData) bool {
	_, err := m.DB.NamedExec(buildQuery("metadata", "delete", metaColumns), &metadata)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (m Meta) DeleteByItem(id string) bool {
	_, err := m.DB.NamedExec("delete from metadata where itemid = :id", id)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (m Meta) BulkDelete(metadata []MetaData) bool {
	tx := m.DB.MustBegin()
	for _, meta := range metadata {
		_, err := tx.NamedExec(buildQuery("metadata", "delete", metaColumns), &meta)
		if err != nil {
			log.Println(err)
			return false
			tx.Rollback()
		}
	}
	err := tx.Commit()
	if err != nil {
		log.Println(err)
		return false
		tx.Rollback()
	}
	return true
}

// #######################################################
//    ENTITY CRUD METHODS
// #######################################################
func (e Entities) Create(entites Entity) (Entity, error) {
	_, err := e.DB.NamedExec(buildQuery("entities", "insert", entityColumns), &entites)
	if err != nil {
		log.Println("Error Adding Entity ", entites.Id)
		log.Println(err)
		return Entity{}, err
	}
	return entites, nil
}

func (e Entities) bulk(entities []Entity) ([]Entity, error) {
	tx := e.DB.MustBegin()
	itemid := ""
	for _, entity := range entities {
		itemid = entity.Itemid
		_, err := tx.NamedExec(buildQuery("metadata", "insert", entityColumns), &entity)
		if err != nil {
			log.Println("Error Adding record ", entity.Id)
			log.Println(err)
			tx.Rollback()
			return []Entity{}, err
		}
	}
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Println("Error committing entities to database rolling back transaction for item id ", itemid)
	}
	return entities, nil
}

func (e Entities) Find() ([]Entity, error) {
	entites := make([]Entity, 0)
	err := e.DB.Select(&entites, buildQuery("entities", "", entityColumns)+" ORDER BY name ASC")
	if err != nil {
		log.Println("Error Finding Feeds")
		log.Println(err)
		return entites, err
	}
	return entites, nil
}

func (e Entities) FindById(id string) (Entity, error) {
	entites := Entity{}
	err := e.DB.Get(&entites, buildQuery("entities", "", entityColumns)+" WHERE id=$1", id)
	if err != nil {
		log.Println(err)
		return entites, err
	}
	return entites, nil
}

func (e Entities) FindBy(field string, value interface{}) ([]Entity, error) {
	entites := make([]Entity, 0)
	queryString := fmt.Sprintf(buildQuery("entities", "", entityColumns)+" WHERE %s=$1", field)
	log.Println(queryString)
	err := e.DB.Select(&entites, queryString, value)
	if err != nil {
		log.Println("Error getting entities")
		log.Println(err)
		return entites, err
	}
	return entites, nil
}

func (e Entities) Update(entites Entity) (Entity, error) {
	_, err := e.DB.NamedExec(buildQuery("entities", "update", entityColumns), &entites)
	if err != nil {
		log.Println(err)
		return Entity{}, err
	}
	return entites, nil
}

func (e Entities) Delete(entites Entity) bool {
	_, err := e.DB.NamedExec(buildQuery("entities", "delete", entityColumns), &entites)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (e Entities) DeleteByItem(id string) bool {
	_, err := e.DB.NamedExec("delete from entities where itemid = :id", id)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (e Entities) BulkDelete(entities []Entity) bool {
	tx := e.DB.MustBegin()
	for _, meta := range entities {
		_, err := tx.NamedExec(buildQuery("entities", "delete", entityColumns), &meta)
		if err != nil {
			log.Println(err)
			return false
			tx.Rollback()
		}
	}
	err := tx.Commit()
	if err != nil {
		log.Println(err)
		return false
		tx.Rollback()
	}
	return true
}
