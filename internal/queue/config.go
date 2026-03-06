package queue

type Config struct {
	Url         string
	MaxMessages int32
	WaitTime    int32
}
