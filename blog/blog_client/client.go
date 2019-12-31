package main

import(
  "context"
  "fmt"
  "io"
  "log"
  "github.com/villegasl/go_grpc_course/blog/blogpb"

  "google.golang.org/grpc"
  "google.golang.org/grpc/status"
  "google.golang.org/grpc/codes"
)

func main() {
  fmt.Println("Starting client\n\n")

  // Creating a client connection
  cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
  if err != nil {
    log.Fatalf("Could not connect: %v", err)
  }
  defer cc.Close()

  c := blogpb.NewBlogServiceClient(cc)

  // doUnary(c)

  // doServerStreaming(c)

  // doClientStreaming(c)

  // doBiDiStreaming(c)

  // doUnaryWithDeadline(c, 5*time.Second) // should complete
  // doUnaryWithDeadline(c, 1*time.Second) // should timeout


  // blogRequests := []*blogpb.CreateBlogRequest {
  //   &blogpb.CreateBlogRequest {
  //     Blog: &blogpb.Blog {
  //       AuthorId: "Son Coomer",
  //       Title: "Son Commer's blog",
  //       Content: "Content of Son Commer's awesome blog",
  //     },
  //   }, &blogpb.CreateBlogRequest {
  //     Blog: &blogpb.Blog {
  //       AuthorId: "CC S.",
  //       Title: "CC's blog",
  //       Content: "Content of CC's awesome blog",
  //     },
  //   }, &blogpb.CreateBlogRequest {
  //     Blog: &blogpb.Blog {
  //       AuthorId: "Axl",
  //       Title: "Axl's blog",
  //       Content: "Content of Axl's awesome blog",
  //     },
  //   }, &blogpb.CreateBlogRequest {
  //     Blog: &blogpb.Blog {
  //       AuthorId: "Trivago developer",
  //       Title: "Trivago developer's blog",
  //       Content: "Content of Trivago developer's awesome blog",
  //     }, 
  //   },
  // }
  // for _, req := range blogRequests {
  //   createBlog(c, req)
  // }

  // deleteBlog(c, uint64(2))
  // readBlog(c, uint64(2))

  listBlog(c)

  // req1 := &blogpb.UpdateBlogRequest {
  //   Blog: &blogpb.Blog {
  //     Id: uint64(1),
  //     AuthorId: "luisg-villegas",
  //     Title: "My first blog",
  //     Content: "Updated content of my first blog",
  //   },
  // }
  // req2 := &blogpb.UpdateBlogRequest {
  //   Blog: &blogpb.Blog {
  //     Id: uint64(5),
  //     AuthorId: "luisg-villegas",
  //     Title: "My first blog",
  //     Content: "Updated content of my first blog",
  //   },
  // }
  // updateBlog(c, req1)
  // updateBlog(c, req2)
}

func createBlog(c blogpb.BlogServiceClient, req *blogpb.CreateBlogRequest) {
  fmt.Println("Create Blog RPC")
  res, err := c.CreateBlog(context.Background(), req)
  if err != nil {
    resErr, ok := status.FromError(err)
    if ok {
      // user error
      if resErr != nil {
        if resErr.Code() == codes.DeadlineExceeded {
          fmt.Println("Timeout was hit: deadline was exceeded\n")
        } else {
          log.Fatalf("%v\n\n", resErr.Err())
        }
      }
    } else {
      // unknown error
      log.Fatalf("Error while calling CreateBlog RPC: %v\n\n", err)
    }
    return
  }
  log.Printf("Response from CreateBlog: %v\n\n", res.GetBlog())
}

func readBlog(c blogpb.BlogServiceClient, id uint64) {
  fmt.Println("Sending Read Blog RPC Request with blog id", id, "\n")
  req := &blogpb.ReadBlogRequest {
    BlogId: id,
  }
  res, err := c.ReadBlog(context.Background(), req)
  if err != nil {
    resErr, ok := status.FromError(err)
    if ok {
      // user error
      if resErr != nil {
        if resErr.Code() == codes.DeadlineExceeded {
          fmt.Println("Timeout was hit: deadline was exceeded\n\n")
        } else {
          log.Fatalf("%v\n\n", resErr.Err())
        }
      }
    } else {
      // unknown error
      log.Fatalf("Error while calling CreateBlog RPC: %v\n\n", err)
    }
    return
  }
  fmt.Printf("Response from ReadBlog: %v\n\n", res)
}

func updateBlog(c blogpb.BlogServiceClient, req *blogpb.UpdateBlogRequest) {
  fmt.Println("Starting Update blog\n\n")

  res, err := c.UpdateBlog(context.Background(), req)
  if err != nil {
    resErr, ok := status.FromError(err)
    if ok {
      // user error
      if resErr != nil {
        if resErr.Code() == codes.DeadlineExceeded {
          fmt.Println("Timeout was hit: deadline was exceeded\n\n")
        } else {
          log.Fatalf("%v\n\n", resErr.Err())
        }
      }
    } else {
      // unknown error
      log.Fatalf("Error while calling UpdateBlog RPC: %v\n\n", err)
    }
    return
  }
  fmt.Printf("Response from UpdateBlog: %v\n\n", res)
}

func deleteBlog(c blogpb.BlogServiceClient, id uint64) {
  req := &blogpb.DeleteBlogRequest {
    BlogId: id,
  }
  res, err := c.DeleteBlog(context.Background(), req)
  if err != nil {
    resErr, ok := status.FromError(err)
    if ok {
      // user error
      if resErr != nil {
        if resErr.Code() == codes.DeadlineExceeded {
          fmt.Println("Timeout was hit: deadline was exceeded\n\n")
        } else {
          fmt.Printf("%v\n\n", resErr.Err())
        }
      }
    } else {
      // unknown error
      log.Fatalf("Error while calling DeleteBlog RPC: %v\n\n", err)
    }
    return
  }
  fmt.Printf("Response from DeleteBlog: %v\n\n", res)
}

func listBlog(c blogpb.BlogServiceClient) {
  fmt.Println("Starting ListBlog RPC server streaming\n\n")
  stream, err := c.ListBlog(context.Background(), &blogpb.ListBlogRequest{})
  if err != nil {
    log.Fatalf("Could not open stream: %v\n\n", err)
  }
  for {
    res, err := stream.Recv()
    if err == io.EOF {
      break
    }
    if err != nil {
      log.Fatalf("%v\n\n",err)
    }
    fmt.Printf("%v\n", res.GetBlog())
  }
}

// func doUnary(c greetpb.GreetServiceClient) {
//   fmt.Println("Starting to do a Unary RPC")
//   req := &greetpb.GreetRequest {
//     Greeting: &greetpb.Greeting {
//       FirstName: "Luis",
//       LastName: "Villegas",
//     },
//   }
//   res, err := c.Greet(context.Background(), req)
//   if err != nil {
//     log.Fatalf("Error while calling Greet RPC: %v", err)
//   }
//   log.Printf("Response from greet: %v", res.GetResult())
// }

// func doServerStreaming(c greetpb.GreetServiceClient) {
//   fmt.Println("Starting to do a Server Streaming RPC")
//   req := &greetpb.GreetManyTimesRequest {
//     Greeting: &greetpb.Greeting {
//       FirstName: "Luis",
//       LastName: "Villegas",
//     },
//   }

//   resStream, err := c.GreetManyTimes(context.Background(), req)
//   if err != nil {
//     log.Fatalf("Error while calling GreetManyTimes RPC: %v", err)
//   }
//   for { 
//     msg, err := resStream.Recv()
//     if err == io.EOF {
//       // we've reached the end of the stream
//       break
//     }
//     if err != nil {
//       log.Fatalf("Error while reading stream: %v", err)
//     }
//     log.Printf("Response from GreetManyTimes: %v\n", msg.GetResult())
//   }
// }

// func doClientStreaming(c greetpb.GreetServiceClient) {
//   fmt.Println("Starting to do a Server Streaming RPC")

//   requests := []*greetpb.LongGreetRequest {
//     &greetpb.LongGreetRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Luis",
//       },
//     }, 
//     &greetpb.LongGreetRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Jhon",
//       },
//     }, 
//     &greetpb.LongGreetRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Lucy",
//       },
//     }, 
//     &greetpb.LongGreetRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Mark",
//       },
//     }, 
//     &greetpb.LongGreetRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Piper",
//       },
//     }, 
//   }

//   stream, err := c.LongGreet(context.Background())
//   if err != nil {
//     log.Fatalf("Error while calling LongGreet: %v", err)
//   }

//   for _, req := range requests {
//     stream.Send(req)
//     time.Sleep(1000 * time.Millisecond)
//     fmt.Printf("Sending request: %v\n", req)
//   }

//   res, err := stream.CloseAndRecv()
//   if err != nil {
//     log.Fatalf("Error while receiving responce from LongGreet: %v", err)
//   }
//   fmt.Printf("LongGreet Response: %v\n",res)
// }

// func doBiDiStreaming(c greetpb.GreetServiceClient) {
//   fmt.Println("Starting to do a Bidirectional Streaming RPC")

//   requests := []*greetpb.GreetEveryoneRequest {
//     &greetpb.GreetEveryoneRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Luis",
//       },
//     }, 
//     &greetpb.GreetEveryoneRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Jhon",
//       },
//     }, 
//     &greetpb.GreetEveryoneRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Lucy",
//       },
//     }, 
//     &greetpb.GreetEveryoneRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Mark",
//       },
//     }, 
//     &greetpb.GreetEveryoneRequest {
//       Greeting: &greetpb.Greeting {
//         FirstName: "Piper",
//       },
//     }, 
//   }

//   // we create a stream by invoking the client
//   stream, err := c.GreetEveryone(context.Background())
//   if err != nil {
//     log.Fatalf("Error while creating stream: %v", err)
//   }
//   waitc := make(chan struct{})

//   // we send a bunch of messages to the server (go routine)
//   go func() {
//     //function to send a bunch of messages
//     for _, req := range requests {
//       log.Printf("Sending message: %v", req)
//       stream.Send(req)
//       time.Sleep(1*time.Second)
//     }
//     stream.CloseSend()
//   }()

//   // we receive a bunch of messages from the server (go routine)
//   go func() {
//     //function to receive a bunch of messages
//     for {
//       res, err := stream.Recv()
//       if err == io.EOF {
//         close(waitc)
//         break
//       } else if err != nil {
//         log.Fatalf("Error while receivin message: %v", err)
//         close(waitc)
//         break
//       }
//       log.Printf("Message received: %v", res.GetResult())
//     }
//   }()

//   // block until everything is done
//   <-waitc
// }

// func doUnaryWithDeadline(c greetpb.GreetServiceClient, timeout time.Duration) {
//   fmt.Printf("Starting to do a Unary with deadline RPC\n\n")
//   req := &greetpb.GreetWithDeadlineRequest {
//     Greeting: &greetpb.Greeting {
//       FirstName: "Luis",
//       LastName: "Villegas",
//     },
//   }
//   ctx, cancel := context.WithTimeout(context.Background(), timeout)
//   defer cancel()

//   res, err := c.GreetWithDeadline(ctx, req)
//   if err != nil {
//     resErr, ok := status.FromError(err)
//     if ok {
//       // user error
//       if resErr != nil {
//         if resErr.Code() == codes.DeadlineExceeded {
//           fmt.Println("Timeout was hit: deadline was exceeded\n")
//         } else {
//           fmt.Printf("Unexpected error: %v\n\n", resErr)
//         }
//       }
//     } else {
//       // unknown error
//       log.Fatalf("Error while calling GreetWithDeadline RPC: %v\n\n", err)
//     }
//     return
//   }
//   log.Printf("Response from GreetWithDeadline: %v\n\n", res.GetResult())
// }