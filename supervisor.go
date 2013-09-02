package processes

import (
    "encoding/json"
    "fmt"
    "github.com/fathomdb/gommons"
    "io/ioutil"
    "log"
    "os"
    "os/exec"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"
)

type WatchedProcessManager struct {
    children map[string]*WatchedProcess

    workDir string
    confDir string

    lock sync.Mutex
}

type WatchedProcessInfo struct {
    Key string
    Pid int
}

type WatchedProcess struct {
    parent *WatchedProcessManager
    key    string
    config *WatchedProcessConfig

    pid     int
    path    string
    done    bool
    lastMod time.Time

    tags map[string]interface{}

    lock sync.Mutex
}

type WatchedProcessConfig struct {
    // These mirror the Cmd arguments
    Name string
    Args []string
    Env  []string
    Dir  string

    // This gets mapped into a Credential
    User string

    // For future use
    Tags map[string]interface{}

    // Used to identity the process in /proc
    MatchExecutableName string

    RestartAction string
}

func NewWatchedProcessManager(workDir string, confDir string) (*WatchedProcessManager, error) {
    s := &WatchedProcessManager{}
    s.children = make(map[string]*WatchedProcess)
    s.workDir = workDir
    s.confDir = confDir

    err := os.MkdirAll(s.PidFilePath(), 0600)
    if err != nil {
        return nil, err
    }
    err = os.MkdirAll(s.LogFilePath(), 0600)
    if err != nil {
        return nil, err
    }

    go s.loadProcesses()

    return s, nil
}

func (s *WatchedProcessManager) GetProcess(key string) *WatchedProcess {
    s.lock.Lock()
    defer s.lock.Unlock()

    child := s.children[key]
    return child
}

func (s *WatchedProcessManager) getProcessesDir() string {
    return s.confDir + "/processes"
}

func fileToKey(file os.FileInfo) (key string) {
    fileName := file.Name()
    key = strings.Replace(fileName, ".conf", "", -1)
    return key
}

func (s *WatchedProcessManager) scanDirectory() error {
    confDir := s.getProcessesDir()

    files, err := gommons.ListDirectory(confDir)
    if err != nil {
        log.Printf("Error listing files in conf dir", err)
        return err
    }

    s.lock.Lock()
    defer s.lock.Unlock()

    for key, child := range s.children {
        if child.isDone() {
            // This is supposedly safe, but the golang designers have been suspiciously quiet about it...
            delete(s.children, key)
        }
    }

    for _, file := range files {
        key := fileToKey(file)
        if key == "" {
            continue
        }

        child := s.children[key]
        if child != nil {
            continue
        }

        path := confDir + "/" + file.Name()

        child = s.buildWatchedProcess(key, path)
        s.children[key] = child
    }

    return nil
}

func (s *WatchedProcessManager) loadProcesses() (err error) {
    for {
        s.scanDirectory()

        time.Sleep(time.Duration(2) * time.Second)
    }

    return nil
}

func (s *WatchedProcessManager) buildWatchedProcess(key string, path string) *WatchedProcess {
    n := &WatchedProcess{}

    n.key = key
    n.parent = s
    n.path = path

    go n.supervise()

    return n
}

func (s *WatchedProcessManager) List() (ret []*WatchedProcessInfo) {
    s.lock.Lock()
    defer s.lock.Unlock()

    ret = []*WatchedProcessInfo{}

    for _, child := range s.children {
        info := child.GetInfo()
        if info == nil {
            continue
        }

        ret = append(ret, info)
    }

    return ret
}

func (s *WatchedProcessManager) WriteProcess(name string, config *WatchedProcessConfig) (err error) {
    var jsonData []byte

    err = gommons.CheckSafeName(name)
    if err != nil {
        return
    }

    jsonData, err = json.Marshal(config)
    if err != nil {
        return
    }

    confPath := s.getProcessesDir() + "/" + name + ".conf"
    err = ioutil.WriteFile(confPath, []byte(jsonData), 0600)
    if err != nil {
        return
    }
    return nil
}

func (s *WatchedProcessManager) DeleteProcess(name string) error {
    err := gommons.CheckSafeName(name)
    if err != nil {
        return err
    }

    confPath := s.getProcessesDir() + "/" + name + ".conf"
    err = gommons.DeleteFile(confPath)
    if err != nil {
        return err
    }

    return nil
}

//func (s *WatchedProcessManager) AddProcess(c *WatchedProcess, persist bool) (err error) {
//	if persist {
//		var jsonData []byte
//
//		jsonData, err = json.Marshal(c)
//		if err != nil {
//			return
//		}
//
//		confPath := s.GetConfDir() + "/" + c.Key + ".conf"
//		err = ioutil.WriteFile(confPath, []byte(jsonData), 0600)
//		if err != nil {
//			return
//		}
//	}
//
//	c.Parent = s
//
//	s.lock.Lock()
//	defer s.lock.Unlock()
//
//	s.Children[c.Key] = c
//
//	go c.RunProcess()
//
//	return nil
//}
//
//func (s *WatchedProcessManager) RemoveProcess(c *WatchedProcess, persist bool) (err error) {
//	if persist {
//		log.Panic("RemoveProcess persist not yet implemented")
//	}
//
//	key := c.Key
//
//	s.lock.Lock()
//	defer s.lock.Unlock()
//	delete(s.Children, key)
//
//	log.Printf("Removing managed process %v\n", c)
//	c.Stop()
//
//	return nil
//}

func readConfig(path string) (*WatchedProcessConfig, error) {
    c := &WatchedProcessConfig{}
    found, err := gommons.ReadJson(path, c)
    if err != nil {
        return nil, err
    }

    if !found {
        return nil, nil
    }

    return c, nil
}

func (s *WatchedProcess) GetInfo() *WatchedProcessInfo {
    s.lock.Lock()
    defer s.lock.Unlock()

    if s.done {
        return nil
    }

    info := &WatchedProcessInfo{}
    info.Key = s.key
    info.Pid = s.pid
    return info
}

func (s *WatchedProcess) setConfig(conf *WatchedProcessConfig) {
    s.lock.Lock()
    defer s.lock.Unlock()
    s.config = conf
}

func (s *WatchedProcess) setPidTags(tags map[string]interface{}) {
    s.lock.Lock()
    defer s.lock.Unlock()

    s.tags = tags
}

func (s *WatchedProcess) GetPid() int {
    s.lock.Lock()
    defer s.lock.Unlock()
    return s.pid
}

func (s *WatchedProcess) setPid(pid int) {
    s.lock.Lock()
    defer s.lock.Unlock()
    s.pid = pid
}

func (s *WatchedProcess) setDone(done bool) {
    s.lock.Lock()
    defer s.lock.Unlock()
    s.done = done
}

func (s *WatchedProcess) isDone() bool {
    s.lock.Lock()
    defer s.lock.Unlock()
    return s.done
}

func (s *WatchedProcess) supervise() {
    var conf *WatchedProcessConfig
    var lastMod time.Time
    var pid int

    stat, err := gommons.StatIfExists(s.path)
    if err != nil {
        log.Printf("Error while checking file stat %v\n", err)
    } else if stat != nil {
        c, err := readConfig(s.path)
        if err != nil {
            log.Printf("Error reading conf file %v", err)
        } else if c == nil {
            log.Printf("Error reading conf file: not found")
        } else {
            conf = c
            lastMod = stat.ModTime()
        }
    }

    pid = s.GetPid()

    if pid <= 0 {
        pid, err = s.ReadPidFile()
        if err != nil {
            log.Printf("Error while reading pid file %v\n", err)
        } else {
            s.setPid(pid)
        }
    }

    tagged := false

    for {
        stat, err := gommons.StatIfExists(s.path)
        if stat == nil || stat.ModTime() != lastMod {
            if pid > 0 {
                if conf.RestartAction == "defer" {
                    // Defer to the new process to kill the old process
                    // e.g. haproxy
                    pid = 0
                    log.Printf("Will defer to new process for restart")
                } else {
                    timeout := 10
                    err = stop(pid, timeout)
                    if err != nil {
                        log.Printf("Error stopping process: %v\n", err)
                    } else {
                        err = gommons.DeleteFile(s.PidFilePath())
                        if err != nil {
                            log.Printf("Error removing pid file: %v\n", err)
                        }

                        if tagged {
                            s.setPidTags(nil)
                            tagged = false
                        }

                        pid = 0
                    }
                }
            }

            if stat == nil {
                conf = nil
            } else {
                c, err := readConfig(s.path)
                if err != nil {
                    log.Printf("Error reading conf file %v", err)

                    // Assume this is because the file was just deleted, or corrupted
                    // Treat corrupted as no-config
                    conf = nil
                } else if c == nil {
                    log.Printf("Error reading conf file: not found")
                } else {
                    conf = c
                    lastMod = stat.ModTime()
                }
            }

            s.setConfig(conf)
        }

        if conf != nil {
            if pid > 0 {
                match, err := conf.isMatchingPid(pid)
                if err != nil {
                    log.Printf("Error while checking if pid matches: %v\n", err)
                } else if !match {
                    log.Printf("Process no longer running...")
                    pid = 0
                } else {
                    if !tagged && conf.Tags != nil {
                        s.setPidTags(conf.Tags)
                        tagged = true
                    }
                }
            }

            if pid <= 0 {
                log.Printf("Starting process: %s\n", s.key)

                logfilepath := s.LogFilePath()
                newPid, err := conf.start(logfilepath)
                if err != nil {
                    log.Printf("Error while starting process %v\n", err)
                } else if newPid != pid {
                    pid = newPid
                    s.setPid(pid)

                    err = s.WritePidFile(pid)
                    if err != nil {
                        log.Printf("Error writing pid file: %v\n", err)
                    }
                }
            }
        }

        if conf == nil && pid <= 0 {
            // conf file deleted; program stopped => exit
            break
        }

        time.Sleep(time.Duration(1) * time.Second)
    }

    log.Printf("Done supervising %s", s.key)
    s.setDone(true)
}

func (s *WatchedProcess) PidFilePath() (path string) {
    return s.parent.PidFilePath() + "/" + s.key + ".pid"
}

func (s *WatchedProcess) LogFilePath() (path string) {
    return s.parent.LogFilePath() + "/" + s.key + ".log"
}

func (s *WatchedProcessManager) PidFilePath() (path string) {
    return s.workDir + "/pids"
}

func (s *WatchedProcessManager) LogFilePath() (path string) {
    return s.workDir + "/logs"
}

func (s *WatchedProcess) WritePidFile(pid int) (err error) {
    var pidFileContents string
    if pid > 0 {
        pidFileContents = strconv.Itoa(pid)
    } else {
        pidFileContents = ""
    }

    err = ioutil.WriteFile(s.PidFilePath(), []byte(pidFileContents), 0600)
    return err
}

func (s *WatchedProcess) ReadPidFile() (pid int, err error) {
    pidFileContents, err := gommons.TryReadTextFile(s.PidFilePath(), "")
    if err != nil {
        return 0, err
    }

    log.Printf("Pid file contained: %s\n", pidFileContents)

    pidFileContents = strings.TrimSpace(pidFileContents)
    if pidFileContents != "" {
        pid, err = strconv.Atoi(pidFileContents)
        if err != nil {
            log.Printf("PID file did not contain a valid pid: %v\n", err)
            pid = 0
            // And continue
        }
    } else {
        pid = 0
    }

    return pid, nil
}

func isRunning(pid int) (match bool, err error) {
    match = false

    procDir := "/proc/" + strconv.Itoa(pid) + "/"
    existingCommand, err := gommons.TryReadTextFile(procDir+"comm", "")

    if err != nil {
        log.Printf("Error reading /proc/<pid>/comm", err)
        return false, nil
    }

    if existingCommand == "" {
        log.Printf("Process not running %v\n", pid)
        return false, nil
    }
    return true, nil
}

func (s *WatchedProcessConfig) isMatchingPid(pid int) (match bool, err error) {
    match = false

    procDir := "/proc/" + strconv.Itoa(pid) + "/"
    existingCommand, err := gommons.TryReadTextFile(procDir+"comm", "")

    if err != nil {
        log.Printf("Error reading /proc/<pid>/comm", err)
        return false, nil
    }

    if existingCommand == "" {
        log.Printf("Process not running %v\n", pid)
    } else {
        existingCommand = strings.TrimSpace(existingCommand)

        // TODO: We should parse cmdline instead, and get rid of all this junk.
        // That way we'd know if the args changed

        if s.MatchExecutableName != "" {
            if strings.Contains(existingCommand, s.MatchExecutableName) {
                match = true
            }
        } else {
            if strings.Contains(existingCommand, s.Name) {
                match = true
            }

            if strings.HasSuffix(s.Name, "/"+existingCommand) {
                match = true
            }

            // The value can be truncated; e.g. superlongcommand => superlongcom
            // The length seems to be 15, but we'll accept anything >= 10
            if len(existingCommand) >= 10 {
                if strings.Contains(s.Name, "/"+existingCommand) {
                    match = true
                } else if strings.HasPrefix(s.Name, existingCommand) {
                    match = true
                }
            }
        }

        if !match {
            log.Printf("Found pid in pidfile, but command did not match: %s vs %s\n", existingCommand, s.Name)
        }
    }

    return match, nil
}

func stop(pid int, timeout int) (err error) {
    count := 0
    for {
        count++

        running, err := isRunning(pid)
        if err != nil {
            return err
        }

        if !running {
            log.Printf("Process no longer running...")
            break
        }

        log.Printf("Sending signal to managed process %v\n", pid)

        sig := syscall.SIGTERM
        if count > timeout {
            sig = syscall.SIGKILL
        }

        err = syscall.Kill(pid, sig)
        if err != nil {
            log.Printf("Error sending signal to process %v\n", err)
        }

        // Note we don't exit the function until the process exits

        time.Sleep(time.Duration(1) * time.Second)
    }

    return nil
}

func (s *WatchedProcessConfig) start(logfilepath string) (pid int, err error) {
    if s.Name == "" {
        return 0, fmt.Errorf("No command specified")
    }

    cmd := exec.Command(s.Name, s.Args...)
    cmd.Env = s.Env
    cmd.Dir = s.Dir

    //type SysProcAttr struct {
    //    14		Chroot     string      // Chroot.
    //    15		Credential *Credential // Credential.
    //    16		Ptrace     bool        // Enable tracing.
    //    17		Setsid     bool        // Create session.
    //    18		Setpgid    bool        // Set process group ID to new pid (SYSV setpgrp)
    //    19		Setctty    bool        // Set controlling terminal to fd 0
    //    20		Noctty     bool        // Detach fd 0 from controlling terminal
    //    21		Pdeathsig  Signal      // Signal that the process will get when its parent dies (Linux only)
    //    22	}

    cmd.SysProcAttr = &syscall.SysProcAttr{}
    cmd.SysProcAttr.Setsid = true

    if s.User != "" {
        cmd.SysProcAttr.Credential, err = BuildCredential(s.User)
        if err != nil {
            log.Printf("Error getting user credentials: %v\n", err)
            return -1, err
        }
        log.Printf("Got credentials %v\n", cmd.SysProcAttr.Credential)
    }

    logfile, err := os.OpenFile(logfilepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
    if err != nil {
        log.Printf("Error creating logfile: %v\n", err)
        return 0, err
    }
    defer logfile.Close()
    cmd.Stdout = logfile
    cmd.Stderr = logfile

    err = cmd.Start()
    if err != nil {
        log.Printf("Error starting command: %v\n", err)
        return 0, err
    }

    // We can't Wait directly ... if we do, when we exit, the spawned process will also exit
    process := cmd.Process
    pid = process.Pid
    log.Printf("Monitoring process.  pid=%d", pid)

    // If we don't wait, we get zombie processes
    go process.Wait()

    return pid, nil
}
