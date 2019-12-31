package main

import(
	"fmt"
	"log"
	"context"
	"io"
	"time"

	"github.com/villegasl/go_grpc_course/calculator/calculatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

func main() {
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to create client connection: %v", err)
	}
	defer cc.Close()

	c := calculatorpb.NewCalculatorServiceClient(cc)

	// doUnary(c)

	// doServerStreaming(c)

	// doClientStreaming(c)

	// doBiDiStreaming(c)

	doErrorUnary(c)
}

func doUnary(c calculatorpb.CalculatorServiceClient) {
	req := &calculatorpb.SumRequest {
		Num1: 10,
		Num2: 3,
	}

	res, err := c.Sum(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling Sum RPC: %v", err)
	}
	fmt.Printf("Result: %v", res.GetResult())
}

func doServerStreaming(c calculatorpb.CalculatorServiceClient) {
	req := &calculatorpb.DecompositeNumberRequest {
		Number: 12390392840,
	}

	resStream, err := c.DecompositeNumber(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling DecompositeNumber: %v", err)
	}
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error while reading stream: %v", err)
		}
		log.Printf("%v ", msg.GetPrimeFactor())
	}
}

func doClientStreaming(c calculatorpb.CalculatorServiceClient) {
	log.Printf("Starting client streaming RPC\n")

	numbers := []int{3,5,9,54,23}

	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("Error while opening stream: %v", err)
	}

	for _, number := range numbers {
		log.Printf("Sending request: %v\n", number)
		err := stream.Send(&calculatorpb.ComputeAverageRequest {
			Number: int64(number),
		})
		if err != nil {
			log.Fatalf("Error while sending request: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while receiving response: %v", err)
	}
	log.Printf("Average: %v", res.GetAverage())
}

func doBiDiStreaming(c calculatorpb.CalculatorServiceClient) {
	log.Printf("Starting bidirectional streaming RPC\n")
	nums := []int32{4, 7, 2, 19, 4, 6, 32}

	stream, err := c.FindMaximum(context.Background())
	if err != nil {
		log.Fatalf("Error while opening stream: %v", err)
	}

	waitc := make(chan struct{})

	// Send a bunch of requests
	go func() {
		for _, num := range nums {
			err = stream.Send(&calculatorpb.FindMaximumRequest{
				Number: num,
			})
			if err != nil {
				log.Fatalf("Error while sending request: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
		err = stream.CloseSend()
		if err != nil {
			log.Fatalf("Error while trying to close send direction of the stream: %v", err)
		}
	}()
	// Receive a bunch of responses
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				log.Printf("Streaming complete\n")
				break
			} else if err != nil {
				log.Fatalf("Error while receiving response: %v", err)
				break
			}
			// log msg received
			log.Printf("%v ", res.GetMax())
		}
		close(waitc)
	}()
	// block until everything is done
	<-waitc
}

func doErrorUnary(c calculatorpb.CalculatorServiceClient) {
	log.Printf("Starting to do a SquareRoot unary RPC\n")

	// correct call
	doErrorCall(c, 10)

	// error call
	doErrorCall(c, -2)
}

func doErrorCall(c calculatorpb.CalculatorServiceClient, n int32) {
	res, err := c.SquareRoot(context.Background(), &calculatorpb.SquareRootRequest{Number: n})

	if err != nil {
		resErr, ok := status.FromError(err)
		if ok {
			// actual error from gRPC (user error)
			fmt.Println("Error message from server:", resErr.Message())
			fmt.Println("Error code:", resErr.Code())
			if resErr.Code() == codes.InvalidArgument {
				fmt.Println("We probably sent a negative number")
				return
			}
		} else {
			log.Fatalf("Big error calling SquareRoot: %v", resErr)
			return
		}
	}
	log.Printf("Result of SquareRoot of %v: %v\n", n, res.GetNumberRoot())
}