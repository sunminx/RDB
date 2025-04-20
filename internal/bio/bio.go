package bio

import (
	"context"
	"sync/atomic"
)

type BioJobType int

const (
	CloseFile BioJobType = iota + 1
	AofFsync
	LazyFree
)

type bioJobWorker struct {
	title    string
	pendings atomic.Int64
	jobCh    chan bioJob
	waits    atomic.Int64
	noticeCh chan struct{}
}

type bioJob struct {
	_type            BioJobType
	arg1, arg2, arg3 any
}

var (
	closeFileWorker bioJobWorker
	aofFsyncWorker  bioJobWorker
	lazyFreeWorker  bioJobWorker
)

func SubmitBioJob(_type BioJobType, arg1, arg2, arg3 any) {
	bioJob := bioJob{_type, arg1, arg2, arg3}
	switch _type {
	case CloseFile:
		closeFileWorker.jobCh <- bioJob
	case AofFsync:
		aofFsyncWorker.jobCh <- bioJob
	case LazyFree:
		lazyFreeWorker.jobCh <- bioJob
	default:
	}
	return
}

func WaitBioJobDone(_type BioJobType, n int64) {
	var worker bioJobWorker
	if _type == CloseFile {
		worker = closeFileWorker
	} else if _type == AofFsync {
		worker = aofFsyncWorker
	} else if _type == LazyFree {
		worker = lazyFreeWorker
	} else {
		return
	}

	if atomic.CompareAndSwapInt64(worker.waits, 0, n) {
		<-worker.noticeCh
	}
	return
}

func BioInit(ctx context.Context) {
	closeFileWorker = createBioJobWorker("close file")
	aofFsyncWorker = createBioJobWorker("aof fsync")
	lazyFreeWorker = createBioJobWorker("lazy free")

	go closeFileWorker.startLoop(ctx, closeFileBioJobChan)
	go aofFsyncWorker.startLoop(ctx, aofFsyncBioJobChan)
	go lazyFreeWorker.startLoop(ctx, lazyFreeBioJobChan)
}

func createBioJobWorker(title string) bioJobWorker {
	return bioJobWorker{
		title:    title,
		pendings: atomic.Int64{},
		waits:    atomic.Int64{},
		jobCh:    make(chan bioJob, 64),
		noticeCh: make(chan struct{}),
	}
}

func (w *bioJobWorker) startLoop(ctx context.Context) {
	var dones int64
	for {
		if atomic.LoadInt64(w.waits) > 0 {
			dones = 0
		}

		select {
		case <-ctx.Done():
			goto _exit
		case job := <-w.jobCh:
			process(job)
			dones++
		}

		if atomic.CompareAndSwapInt64(w.waits, dones, 0) {
			w.noticeCh <- struct{}
		}
	}
_exit:
}

func process(job bioJob) {
	switch job._type {
	case CloseFile:
		processCloseFile(job)
	case AofFsync:
		processAofFsync(job)
	case LazyFree:
		processLazyFree(job)
	default:
	}
	return
}

func processCloseFile(job bioJob) {

}

func processAofFsync(job bioJob) {

}

func processLazyFree(job bioJob) {

}
