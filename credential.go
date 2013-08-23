package processes

import (
    "errors"
    "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
    "syscall"
)

/**
We could also use os.user, which should work with PAM.
This has the advantage of not using Cgo.  I also wrote it before I found os.user
*/
func BuildCredential(username string) (cred *syscall.Credential, err error) {
    username = strings.TrimSpace(username)

    uid := -1
    primaryGid := -1

    {
        passwdContents, err := ioutil.ReadFile("/etc/passwd")
        if err != nil {
            return nil, err
        }
        for _, line := range strings.Split(string(passwdContents), "\n") {
            tokens := strings.Split(line, ":")
            if len(tokens) >= 4 {
                if username == tokens[0] {
                    uid, err = strconv.Atoi(tokens[2])
                    if err != nil {
                        return nil, err
                    }
                    primaryGid, err = strconv.Atoi(tokens[3])
                    if err != nil {
                        return nil, err
                    }
                    break
                }
            }
        }
    }

    if uid < 0 || primaryGid < 0 {
        return nil, errors.New("User not found")
    }

    // We can't set a uid if we're not root;
    // so if we're already running as the correct user then don't pass credentials
    if uid == os.Getuid() {
        log.Printf("Already running as username, won't pass credentials")
        return nil, nil
    }

    groups := make([]uint32, 0, 8)
    {
        groupContents, err := ioutil.ReadFile("/etc/group")
        if err != nil {
            return nil, err
        }

        for _, line := range strings.Split(string(groupContents), "\n") {
            tokens := strings.Split(line, ":")
            if len(tokens) == 4 {
                userList := tokens[3]
                for _, user := range strings.Split(userList, ",") {
                    user = strings.TrimSpace(user)
                    if user == username {
                        gid, err := strconv.Atoi(tokens[2])
                        if err != nil {
                            return nil, err
                        }
                        if gid != primaryGid {
                            groups = append(groups, uint32(gid))
                        }
                    }
                }
            }
        }
    }

    cred = &syscall.Credential{}
    cred.Uid = uint32(uid)
    cred.Gid = uint32(primaryGid)
    cred.Groups = groups

    return cred, nil
}
