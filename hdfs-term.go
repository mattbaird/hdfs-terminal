package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/vladimirvivien/gowfs"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"
)

var uname string

func init() {
	u, _ := user.Current()
	uname = u.Username
}

var debug bool = false
var homePath string
var directoryStack Stack = NewStack()
var OLDPWD string

// 2 stacks, one maintains the current tree,
// todo: pushd/popd
// todo: put/get
// todo: less
// todo: create file
// todo: append to file
// todo: delete file
// todo: file stat
// todo: chown/chgrp/chmod
// connect like this> hdfs mattb@192.168.99.9:50070
func main() {
	flag.Parse()
	connectString := flag.Arg(0)
	var user string = uname
	var host string = "localhost"
	var port int = 50070
	if len(connectString) > 0 {
		if strings.Index(connectString, "@") == -1 {
			user = uname
			host = connectString
		} else {
			connectParams := strings.Split(connectString, "@")
			user = connectParams[0]
			temp := connectParams[1]
			if strings.Index(temp, ":") == -1 {
				host = temp
			} else {
				hostPort := strings.Split(temp, ":")
				host = hostPort[0]
				var err error
				port, err = strconv.Atoi(hostPort[1])
				if err != nil {
					log.Printf("error parsing port:%v", err)
					os.Exit(0)
				}
			}
		}
	}
	var nameNode string = fmt.Sprintf("%s:%v", host, port)
	var path = flag.String("path", fmt.Sprintf("/user/%s", user), "HDFS file path")
	homePath = *path
	OLDPWD = homePath

	conf := *gowfs.NewConfiguration()

	conf.Addr = nameNode
	conf.User = user
	fs, err := gowfs.NewFileSystem(conf)
	if err != nil {
		log.Fatal(err)
	}

	testConnection(fs)
	buildStack(*path)
	for {
		fmt.Print("⇒ ")
		bio := bufio.NewReader(os.Stdin)
		bytes, hasMoreInLine, err := bio.ReadLine()
		cmd := strings.Split(string(bytes), " ")
		if debug {
			log.Printf("hasmore:%v, err:%v, cmd:%s", hasMoreInLine, err, cmd)
		}
		if len(cmd) > 0 {
			switch cmd[0] {
			case "ls":
				ls(fs, currentDir())
			case "cd":
				if len(cmd) > 1 {
					newDir := cmd[1]
					cd(fs, newDir)
				}
			case "quit":
				os.Exit(0)
			case "exit":
				os.Exit(0)
			default:
				fmt.Printf("command not found: %s\n", cmd[0])
			}
		}
		if debug {
			fmt.Println("command is: %s", cmd)
		}
	}
}

func testConnection(fs *gowfs.FileSystem) {
	_, err := fs.ListStatus(gowfs.Path{Name: "/"})
	if err != nil {
		log.Fatal("Unable to connect to server. ", err)
	}
	log.Printf("Connected to server %s... OK.\n", fs.Config.Addr)
}

//http://ss64.com/bash/pushd.html
func pushd(fs *gowfs.FileSystem, hdfsPath string) {
}

func popd(fs *gowfs.FileSystem, hdfsPath string) {
}

func cd(fs *gowfs.FileSystem, hdfsPath string) {
	expandedPath := strings.Replace(hdfsPath, "~", homePath, -1)
	switch expandedPath {
	case "":
		// to to home
		buildStack(homePath)
	case "-":
		// go to $OLDPWD
		_, err := fs.GetFileStatus(gowfs.Path{Name: OLDPWD})
		if err != nil {
			fmt.Printf("%v", err)
		} else {
			// potentially put it on relative stack as well
			buildStack(OLDPWD)
		}
	case ".":
		// go to home directory
		buildStack(homePath)
	case "..":
		// go up one dir in relative
		directoryStack.Pop()
		currentDir()
	default:
		if strings.HasPrefix(expandedPath, "/") {
			// absolute directory
			_, err := fs.GetFileStatus(gowfs.Path{Name: expandedPath})
			if err != nil {
				fmt.Printf("%v", err)
			} else {
				// potentially put it on relative stack as well
				buildStack(expandedPath)
			}
		} else {
			// relative directory
			p := fmt.Sprintf("%s/%s", currentDir(), expandedPath)
			_, err := fs.GetFileStatus(gowfs.Path{Name: p})
			if err != nil {
				fmt.Println(err.Error())
			} else {
				buildStack(p)
			}
		}
	}
}

func ls(fs *gowfs.FileSystem, hdfsPath string) {
	stats, err := fs.ListStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to list paths: ", err)
	}
	if debug {
		log.Printf("Found %d file(s) at %s\n", len(stats), hdfsPath)
	}
	for _, stat := range stats {
		fmt.Printf(
			"%-11s %3s %s\t%s\t%11d %20v %s\n",
			formatFileMode(stat.Permission, stat.Type),
			formatReplication(stat.Replication, stat.Type),
			stat.Owner,
			stat.Group,
			stat.Length,
			formatModTime(stat.ModificationTime),
			stat.PathSuffix)
	}
}

func buildStack(path string) error {
	OLDPWD = currentDir()
	splitPath := strings.Split(path, "/")
	directoryStack.Clear()
	for _, pathPart := range splitPath {
		directoryStack.Push(&Node{Value: pathPart})
	}
	return nil
}

func currentDir() string {
	var buffer bytes.Buffer
	for _, pathPart := range directoryStack.PeekNodes() {
		if pathPart != nil && pathPart.Value != "" {
			buffer.WriteString("/")
			buffer.WriteString(pathPart.Value)
		}
	}
	if debug {
		log.Printf("currentDir:%v", buffer.String())
	}
	return buffer.String()
}

func formatFileMode(webfsPerm string, fileType string) string {
	perm, _ := strconv.ParseInt(webfsPerm, 8, 16)
	fm := os.FileMode(perm)
	if fileType == "DIRECTORY" {
		fm = fm | os.ModeDir
	}
	return fm.String()
}

func formatReplication(rep int64, fileType string) string {
	repStr := strconv.FormatInt(rep, 8)
	if fileType == "DIRECTORY" {
		repStr = "-"
	}
	return repStr
}

func formatModTime(modTime int64) string {
	modTimeAdj := time.Unix((modTime / 1000), 0) // adjusted for Java Calendar in millis.
	return modTimeAdj.Format("2006-01-02 15:04:05")
}
