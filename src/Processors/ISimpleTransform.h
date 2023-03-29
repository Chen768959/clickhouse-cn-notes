#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** Has one input and one output.
  * Simply pull a block from input, transform it, and push it to output.
  */
class ISimpleTransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;

    Port::Data input_data;
    Port::Data output_data;
    bool has_input = false;
    bool has_output = false;
    bool no_more_data_needed = false;
    const bool skip_empty_chunks;

    /// Set input port NotNeeded after chunk was pulled.
    /// Input port will become needed again only after data was transformed.
    /// This allows to escape caching chunks in input port, which can lead to uneven data distribution.
    bool set_input_not_needed_after_read = true;

    // “需由子类实现，每个子类可借由此方法，对input chunk本身做一个处理。然后Simple算子会将处理后的chunk交由output”
    // SimpleTransform的work()方法会将input的chunk传入此transform中
    // 子类的transform()方法对 input chunk 处理后
    // SimpleTransform再将input chunk直接作为output的chunk传输出去
    virtual void transform(Chunk &)
    {
        throw Exception("Method transform is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void transform(Chunk & input_chunk, Chunk & output_chunk)
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
    }

    virtual bool needInputData() const { return true; }
    void stopReading() { no_more_data_needed = true; }

public:
    ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    void setInputNotNeededAfterRead(bool value) { set_input_not_needed_after_read = value; }
};

}
