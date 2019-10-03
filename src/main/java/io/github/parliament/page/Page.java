package io.github.parliament.page;

import com.google.common.base.Preconditions;
import lombok.*;

/**
 * 见{@link Pager}
 */
@EqualsAndHashCode
@ToString
public class Page {
    @Getter
    private int no;
    @Getter
    private int location;
    @Getter
    private byte[] content;

    @Builder
    public Page(int no, int location, @NonNull byte[] content) {
        Preconditions.checkArgument(no >= 0);
        Preconditions.checkArgument(location > 0);
        this.no = no;
        this.location = location;
        this.content = content;
    }

    // include i. exclude j
    public synchronized void replaceBytes(int i, int j, byte[] src) {
        Preconditions.checkArgument(i < j);
        Preconditions.checkArgument(j - i == src.length);
        Preconditions.checkArgument(j <= content.length);

        int c = 0;
        while (i < j) {
            content[i] = src[c];
            c++;
            i++;
        }
    }

    public synchronized void updateContent(byte[] content) {
        this.content = content;
    }
}
