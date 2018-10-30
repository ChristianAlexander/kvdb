package serializable

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

// A lockerMap holds a locker for each key, and the keys for each transaction.
type lockerMap struct {
	mu      sync.RWMutex
	lockers map[string]*keyLocker
	keys    map[int64][]string
}

// A keyLocker is the implementation of a lock for a given key.
type keyLocker struct {
	mu sync.Mutex

	waitingReaders []waiter
	waitingWriters []waiter

	// activeTransactions stores the active transaction IDs
	activeTransactions map[int64]struct{}

	// writeLockTxID is set if a transaction has an exclusive lock
	writeLockTxID int64
}

type waiter struct {
	txID  int64
	ready chan struct{}
}

func (lm *lockerMap) getKeyLocker(key string) *keyLocker {
	lm.mu.Lock()
	locker, ok := lm.lockers[key]
	if !ok {
		l, ok := lm.lockers[key]
		if !ok {
			lm.lockers[key] = &keyLocker{
				activeTransactions: make(map[int64]struct{}),
			}
			locker = lm.lockers[key]
		} else {
			locker = l
		}
	}
	lm.mu.Unlock()

	return locker
}

func (lm *lockerMap) setTxKey(txID int64, key string) {
	lm.mu.Lock()
	lm.keys[txID] = append(lm.keys[txID], key)
	lm.mu.Unlock()
}

// Acquire gets an exclusive lock on a key for a transaction.
func (lm *lockerMap) Acquire(ctx context.Context, txID int64, key string) error {
	locker := lm.getKeyLocker(key)
	logrus.WithField("txID", txID).Debugf("Acquiring write lock for %s", key)

	for {
		locker.mu.Lock()
		if locker.writeLockTxID == txID {
			locker.mu.Unlock()
			logrus.WithField("txID", txID).Debugf("Write lock acquired for %s, already write holder", key)
			return nil
		}

		activeTxCount := len(locker.activeTransactions)
		if activeTxCount == 0 {
			locker.activeTransactions[txID] = struct{}{}
			lm.setTxKey(txID, key)
			locker.writeLockTxID = txID
			locker.mu.Unlock()
			logrus.WithField("txID", txID).Debugf("Write lock acquired for %s", key)
			return nil
		} else if activeTxCount == 1 {
			if _, ok := locker.activeTransactions[txID]; ok {
				locker.writeLockTxID = txID
				locker.mu.Unlock()
				return nil
			}
		}

		ready := make(chan struct{})
		me := waiter{txID, ready}
		locker.waitingWriters = append(locker.waitingWriters, me)
		locker.mu.Unlock()

		logrus.WithField("txID", txID).Debugf("Entering write wait for '%s'", key)
		select {
		case <-ctx.Done():
			locker.mu.Lock()
			ww := locker.waitingWriters
			for i, w := range ww {
				if w.txID == txID {
					locker.waitingWriters = append(locker.waitingWriters[:i], locker.waitingWriters[i+1:]...)
				}
			}
			locker.mu.Unlock()
			return ctx.Err()
		case <-ready:
			break
		}
	}
}

// RAcquire gets a shared lock on a key for a transaction.
func (lm *lockerMap) RAcquire(ctx context.Context, txID int64, key string) error {
	locker := lm.getKeyLocker(key)
	logrus.WithField("txID", txID).Debugf("Acquiring read lock for %s", key)

	for {
		locker.mu.Lock()
		if locker.writeLockTxID == txID {
			locker.mu.Unlock()
			logrus.WithField("txID", txID).Debugf("Read lock acquired for %s, already write holder", key)
			return nil
		}

		_, txExists := locker.activeTransactions[txID]
		if locker.writeLockTxID == 0 && (txExists || len(locker.waitingWriters) == 0) {
			locker.activeTransactions[txID] = struct{}{}
			lm.setTxKey(txID, key)
			locker.mu.Unlock()
			logrus.WithField("txID", txID).Debugf("Read lock acquired for %s", key)
			return nil
		}

		ready := make(chan struct{})
		me := waiter{txID, ready}
		locker.waitingReaders = append(locker.waitingReaders, me)
		locker.mu.Unlock()

		logrus.WithField("txID", txID).Debugf("Entering read wait for '%s'", key)
		select {
		case <-ctx.Done():
			wr := locker.waitingReaders
			for i, w := range wr {
				if w.txID == txID {
					locker.waitingReaders = append(locker.waitingReaders[:i], locker.waitingReaders[i+1:]...)
				}
			}
			locker.mu.Unlock()
			return ctx.Err()
		case <-ready:
			break
		}
	}
}

// Release gives up all locks held by a transaction.
func (lm *lockerMap) Release(txID int64) {
	logrus.WithField("txID", txID).Debug("Releasing transaction")
	keys, ok := lm.keys[txID]
	if !ok {
		return
	}

	for _, key := range keys {
		logrus.WithField("txID", txID).Debugf("Releasing from key %s", key)
		locker := lm.getKeyLocker(key)

		locker.mu.Lock()
		delete(locker.activeTransactions, txID)

		if locker.writeLockTxID == txID {
			locker.writeLockTxID = 0

			for _, r := range locker.waitingReaders {
				logrus.WithField("txID", txID).Debugf("Releasing reader for tx %d - key %s", r.txID, key)
				close(r.ready)
			}
			locker.waitingReaders = nil
		}

		var hasUnlockedWriter bool
		if len(locker.activeTransactions) == 0 {
			ww := locker.waitingWriters
			for i, w := range ww {
				if w.txID == txID {
					close(w.ready)
					hasUnlockedWriter = true
					locker.waitingWriters = append(locker.waitingWriters[:i], locker.waitingWriters[i+1:]...)
				}
			}
		}

		if !hasUnlockedWriter && len(locker.waitingWriters) != 0 {
			w := locker.waitingWriters[0]
			locker.waitingWriters = locker.waitingWriters[1:]
			close(w.ready)
		}
		locker.mu.Unlock()
	}

	lm.mu.Lock()
	delete(lm.keys, txID)
	lm.mu.Unlock()
}
