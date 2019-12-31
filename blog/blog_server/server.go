package main

import(
  "fmt"
  "log"
  "net"
  "os"
  "context"
  "os/signal"
  "encoding/binary"

  "github.com/villegasl/go_grpc_course/blog/blogpb"
  "github.com/boltdb/bolt"
  "github.com/golang/protobuf/proto"
  "google.golang.org/grpc"
  "google.golang.org/grpc/status"
  "google.golang.org/grpc/codes"
)

type server struct{
  db *bolt.DB
}

type blogItem struct {
  ID        string
  AuthorID  string
  Content   string
  Title     string
}

func NewBlogServer(filepath string) *server {  
  // Open the Bolt database "blog.db" located in the filepath directory.
  // It will be created if it doesn't exist.
  fmt.Println("Connecting to Bolt")
  BoltDB, err := bolt.Open(filepath+"/blog.db", 0600, nil)
  if err != nil {
    log.Fatal(err)
  }
  return &server{BoltDB}
}

func (s *server) Close() {
  fmt.Println("Closing Bolt")
  s.db.Close()
}

func (s *server) setupDB() {
  s.db.Update(func(tx *bolt.Tx) error {
    _, err := tx.CreateBucketIfNotExists([]byte("Blog"))
    if err != nil {
      return fmt.Errorf("Could not create bucket: %s", err)
    }
    return nil
  })
}

func (s *server) ListBlog(req *blogpb.ListBlogRequest, stream blogpb.BlogService_ListBlogServer) error {
  fmt.Printf("ListBlog was invoked\n\n")

  return s.db.View(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("Blog"))
    c := b.Cursor()

    for k, v := c.First(); k != nil; k, v = c.Next() {
      blog := &blogpb.Blog{}
      err := proto.Unmarshal(v, blog)
      if err != nil {
        return status.Error(codes.Internal, fmt.Sprintf("Unmarshal error: %v\n", err))
      }
      stream.Send(&blogpb.ListBlogResponse {
        Blog: blog,
      })
    }
    return nil
  })
}

func (s *server) DeleteBlog(ctx context.Context, req *blogpb.DeleteBlogRequest) (*blogpb.DeleteBlogResponse, error) {
  fmt.Printf("DeleteBlog was invoked with: %v\n\n", req)
  id := uitob(req.GetBlogId())

  // printing all the blogs for debuging purposes
  err := s.db.View(func(tx *bolt.Tx) error {
    fmt.Printf("Before deleting blog number %v\n\n", req.GetBlogId())
    b := tx.Bucket([]byte("Blog"))
    c := b.Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
      blog := &blogpb.Blog{}
      err := proto.Unmarshal(v, blog)
      if err != nil {
        return err
      }
      fmt.Printf("Key=%v, Value=%v\n", k, blog)
    }
    return nil
  })
  if err != nil {
    log.Fatalf("Something went wrong: %v\n\n", err)
  }

  err = s.db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("Blog"))

    blogBytes := b.Get(id)
    if blogBytes == nil {
      fmt.Printf("Could not find blog with id %v\n\n", btoui(id))
      return status.Error(codes.NotFound, fmt.Sprintf("Could not find blog with id %v\n", btoui(id)))
    }

    return b.Delete(id)
  })
  if err != nil {
    return nil, err
  }
  // printing all the blogs for debuging purposes
  err = s.db.View(func(tx *bolt.Tx) error {
    fmt.Printf("After deleting blog number %v\n\n", req.GetBlogId())
    b := tx.Bucket([]byte("Blog"))
    c := b.Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
      blog := &blogpb.Blog{}
      err := proto.Unmarshal(v, blog)
      if err != nil {
        return err
      }
      fmt.Printf("Key=%v, Value=%v\n", k, blog)
    }
    return nil
  })
  if err != nil {
    log.Fatalf("Something went wrong: %v\n\n", err)
  }

  return &blogpb.DeleteBlogResponse {
    BlogId: req.GetBlogId(),
  }, nil
}

func (s *server) UpdateBlog(ctx context.Context, req *blogpb.UpdateBlogRequest) (*blogpb.UpdateBlogResponse, error) {
  fmt.Printf("UpdateBlog was invoked with: %v\n\n", req)
  
  blog := req.GetBlog()
  id := uitob(blog.GetId())

  err := s.db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("Blog"))

    blogBytes := b.Get(id)
    if blogBytes == nil {
      fmt.Printf("Could not find blog with id %v\n\n", btoui(id))
      return status.Error(codes.NotFound, fmt.Sprintf("Could not find blog with id %v\n", btoui(id)))
    }

    // Marshal blog post into protocol buffer
    serializedBlogPost, err := proto.Marshal(blog)
    if err != nil {
      return err
    }
    // save blog post to the DB
    return b.Put(id, serializedBlogPost)
  })
  if err != nil {
    return nil, err
  }
  fmt.Printf("Blog %v updated successfully\n\n", blog.GetId())
  return &blogpb.UpdateBlogResponse {
    Blog: blog,
  }, nil
}

func (s *server) ReadBlog(ctx context.Context, req *blogpb.ReadBlogRequest) (*blogpb.ReadBlogResponse, error) {
  fmt.Printf("ReadBlog was invoked with: %v\n\n", req)

  id := uitob(req.GetBlogId())
  blog := &blogpb.Blog{}

  err := s.db.View(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("Blog"))

    blogBytes := b.Get(id)
    if blogBytes == nil {
      return status.Error(codes.NotFound, fmt.Sprintf("Could not find blog with id %v\n", btoui(id)))
    }
    err := proto.Unmarshal(blogBytes, blog)
    if err != nil {
      return status.Error(codes.Internal, fmt.Sprintf("Error while Unmarshaling: %v\n", err))
    }
    return nil
  })
  if err != nil {
    return nil, err
  }

  return &blogpb.ReadBlogResponse {
    Blog: blog,
  }, nil
}

func (s *server) CreateBlog(ctx context.Context, req *blogpb.CreateBlogRequest) (*blogpb.CreateBlogResponse, error) {
  fmt.Printf("CreateBlog was invoked with: %v\n\n", req)
  blog := req.GetBlog()

  err := s.db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("Blog"))
    // Generate ID for the blog post.
    blog.Id, _ = b.NextSequence()
    // Marshal blog post into protocol buffer
    serializedBlogPost, err := proto.Marshal(blog)
    if err != nil {
      return err
    }
    // save blog post to the DB
    return b.Put(uitob(blog.Id), serializedBlogPost)
  })
  if err != nil {
    log.Fatalf("Could not create blog: %v", err)
    return nil, status.Error(codes.Internal, fmt.Sprintf("Internal error: %v", err))
  }
  return &blogpb.CreateBlogResponse {
    Blog: blog,
  }, nil
}

func uitob(v uint64) []byte {
  b := make([]byte, 8)
  binary.BigEndian.PutUint64(b, v)
  return b
}

func btoui(b []byte) uint64 {
  return binary.BigEndian.Uint64(b)
}

func main() {
  // if we crash the go code, we get the file name and line number
  log.SetFlags(log.LstdFlags | log.Lshortfile)

  blogServer := NewBlogServer("database")
  defer blogServer.Close()

  // create Blog collection
  blogServer.setupDB()

  fmt.Println("Blog Service Started")

  lis, err := net.Listen("tcp", "0.0.0.0:50051")
  if err != nil {
    log.Fatalf("Failed to listen: %v", err)
  }

  s := grpc.NewServer()
  blogpb.RegisterBlogServiceServer(s, blogServer)

  go func() {
    fmt.Println("Starting Server...\n")
    if err := s.Serve(lis); err != nil {
      log.Fatalf("failed to serve: %v", err)
    }
  }()

  // Wait for Control C to exit
  ch := make(chan os.Signal, 1)
  signal.Notify(ch, os.Interrupt)

  // Block until a signal is received
  <-ch
  fmt.Println("Stopping the server")
  s.Stop()
  fmt.Println("Closing the listener")
  lis.Close()
  fmt.Println("End of Program")
}