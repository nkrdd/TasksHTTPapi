package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Task struct {
	ID int `json:"id"`
	Title string `json:"title"`
	Done bool `json:"done"`
}

type TaskHandler struct {
	tasks []Task
	mu sync.RWMutex
	nextID int
}

type UpdateTaskRequest struct {
	Title *string `json:"title"`
	Done *bool `json:"done"`
}

type createTaskRequest struct {
	Title string `json:"title"`
}

func (h *TaskHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		h.mu.RLock()
		tasks := append([]Task(nil), h.tasks...)
		h.mu.RUnlock()
		q := r.URL.Query()
		if val, ok := q["done"]; ok {
			cond, err := strconv.ParseBool(val[0]) 
			if err != nil {
				log.Printf("parse query param: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			filteredTasks := make([]Task, 0) 
			for _, task := range tasks {
				if task.Done == cond {
					filteredTasks = append(filteredTasks, task)
				}
			}
			tasks = filteredTasks
		}

		if err := json.NewEncoder(w).Encode(tasks); err != nil {
			fmt.Printf("get tasks: %v", err)
			return
		}
	case http.MethodPost:
		task := createTaskRequest{}

		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			fmt.Printf("bad json: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if task.Title == "" {
			fmt.Printf("empty title task")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		
		h.mu.Lock()
		newTask := Task{Title: task.Title, ID: h.nextID}
		h.nextID++
		h.tasks = append(h.tasks, newTask)
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)

		if err := json.NewEncoder(w).Encode(newTask); err != nil {
			fmt.Printf("return new task: %v", err)
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *TaskHandler) findTaskByID(id int) (Task, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, task := range h.tasks {
		if task.ID == id {
			return task, true
		}
	}
	return Task{}, false
}

func (h *TaskHandler) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	value := r.PathValue("id")
	id, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("get id from request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	if id <= 0 {
		log.Printf("id must be positive")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	
	task, isExist := h.findTaskByID(id)
	if !isExist {
		log.Printf("task with id: %d not exist!", id)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(task); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *TaskHandler) deleteTaskByID(id int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, val := range h.tasks {
		if val.ID == id {
			h.tasks = append(h.tasks[:i], h.tasks[i+1:]...)
			return true
		}
	}
	return false
}

func (h *TaskHandler) deleteByID(w http.ResponseWriter, r *http.Request) {
	value := r.PathValue("id")
	id, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("get id from request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	if id <= 0 {
		log.Printf("id must be positive")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if successDelete := h.deleteTaskByID(id); successDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func (h *TaskHandler) updateTaskByID(id int, update UpdateTaskRequest) (Task, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, val := range h.tasks {
		if val.ID == id {
			if update.Done != nil {
				h.tasks[i].Done = *update.Done
			}
			if update.Title != nil {
				h.tasks[i].Title = *update.Title
			}
			return h.tasks[i], true
		}
	}
	return Task{}, false
}

func (h *TaskHandler) updateTask(w http.ResponseWriter, r *http.Request) {
	value := r.PathValue("id")
	id, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("get id from request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	if id <= 0 {
		log.Printf("id must be positive")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	update := UpdateTaskRequest{}
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("fetch task request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	if update.Done == nil && update.Title == nil {
		log.Printf("empty new update")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if update.Title != nil {	
		if *update.Title == "" {
			log.Printf("empty new title")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if task, changed := h.updateTaskByID(id, update); changed {
		result, err := json.Marshal(task)
		if err != nil {
			log.Printf("marshal task to json: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(result)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		fmt.Println(r.Method, r.URL.Path, duration)
	})
}
func main() {
	mux := http.NewServeMux()
	tasksList := []Task{
		{ID: 1, Title: "learn net/http", Done: false},
		{ID: 2, Title: "sleep", Done: false},
		{ID: 3, Title: "writing code", Done: true},
	}
	
	h := &TaskHandler{tasks: tasksList, nextID: 4}

	mux.HandleFunc("/health", health)
	mux.Handle("/tasks", h)
	mux.HandleFunc("GET /tasks/{id}", h.handleTaskByID)
	mux.HandleFunc("DELETE /tasks/{id}", h.deleteByID)
	mux.HandleFunc("PATCH /tasks/{id}", h.updateTask)

	server := &http.Server{
		Addr: ":8080",
		Handler: loggingMiddleware(mux),
		ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout: 15 * time.Second,
	}
	
	go func() {	
		log.Println("server starting on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("server error: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit
	fmt.Println("received a stop signal, stopping the server")
	
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(shutdownCtx)
	if err != nil {
		fmt.Println("failed to wait for ongoing requests")
	} else {
		fmt.Println("server shut down gracefully")
	}

}
