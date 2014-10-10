package com.benstopford.nosql.tests;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.validators.RowValidator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class RandomReadTest {

    @Test
    public void shouldReadBatchOfOne() throws Exception {
        DB db = mock(DB.class);
        int batch = 1;
        int readCount = 10;
        RandomRead randomRead = new RandomRead(new Main.RunState(), db, Long.MAX_VALUE, readCount, 10, batch, RowValidator.NULL);
        randomRead.execute();

        verify(db, times(10)).read(anyCollection(), anyObject());
    }

    @Test
    public void shouldReadLargerBatches() throws Exception {
        DB db = mock(DB.class);
        int batch = 10;
        int readCount = 100;
        RandomRead randomRead = new RandomRead(new Main.RunState(), db, Long.MAX_VALUE, readCount, 10, batch, RowValidator.NULL);
        randomRead.execute();

        verify(db, times(10)).read(anyCollection(), anyObject());
    }

    @Test
    public void shouldPassRemainderWhenBatchesDontDivideEvenly() throws Exception {
        DB db = mock(DB.class);
        int batch = 2;
        int readCount = 9;
        RandomRead randomRead = new RandomRead(new Main.RunState(), db, Long.MAX_VALUE, readCount, 10, batch, RowValidator.NULL);
        randomRead.execute();

        //should call 5 times
        ArgumentCaptor<Set> captor = ArgumentCaptor.forClass(Set.class);
        verify(db, times(5)).read(captor.capture(), anyObject());

        //and last batch should be only of size ONE
        assertEquals(1, captor.getValue().size());
    }


}
