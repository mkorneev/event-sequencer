package com.mkorneev.event_sequencer

import java.time.{Duration, LocalDateTime}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

class EventSequencerFlow(window: Duration)
  extends GraphStage[FlowShape[UserAuthEvent, (String, EventsSeq[String])]] {

  private val in = Inlet[UserAuthEvent]("EventSequencerFlow.in")
  private val out = Outlet[(String, EventsSeq[String])]("EventSequencerFlow.out")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private val sequencer = new EventSequencer[String, String](window)

    override def onPush(): Unit = {
      val event = grab(in)

      sequencer.put(event.date, event.ip, event.username)

      // Filter out sequences of 1 element
      val completedSequences = sequencer.removeCompletedSequences(event.date).filter(_._2.values.size > 1)

      if (completedSequences.isEmpty) {
        pull(in)
      } else {
        emitMultiple(out, completedSequences.toIterator)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      val completedSequences = sequencer.removeCompletedSequences(LocalDateTime.MAX).filter(_._2.values.size > 1)

      if (isAvailable(out)) emitMultiple(out, completedSequences.toIterator)
      completeStage()
    }

    setHandler(in, this)
    setHandler(out, this)
  }

  override def shape: FlowShape[UserAuthEvent, (String, EventsSeq[String])] = FlowShape(in, out)
}