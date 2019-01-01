package com.mkorneev.bz

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

      sequencer.add(event.date, event.ip, event.username)

      // Get only auth sequences of more than 1 element
      val closedSequences = sequencer.takeClosedSequences(event.date).filter(_._2.values.size > 1)

      if (closedSequences.isEmpty) {
        pull(in)
      } else {
        emitMultiple(out, closedSequences.toIterator)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      val closedSequences = sequencer.takeClosedSequences(LocalDateTime.MAX).filter(_._2.values.size > 1)

      if (isAvailable(out)) emitMultiple(out, closedSequences.toIterator)
      completeStage()
    }

    setHandler(in, this)
    setHandler(out, this)
  }

  override def shape: FlowShape[UserAuthEvent, (String, EventsSeq[String])] = FlowShape(in, out)
}